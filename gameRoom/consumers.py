import asyncio
import json
import threading
import time
import uuid

from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.conf import settings
from redis.asyncio import Redis

ROOM_GROUP = 'game-room'
MAX_PLAYERS = 5
PLAYER_LEASE_SECONDS = 90
HEARTBEAT_SECONDS = 30
MOVE_INTERVAL_SECONDS = 0.05
MAX_CHAT_LENGTH = 300
ROOM_MEMBERS_KEY = 'game-room:members'
ROOM_PLAYERS_KEY = 'game-room:players'

_local_players = {}
_local_players_lock = threading.Lock()
_redis_client = None

ADMIT_PLAYER_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, player_id in ipairs(expired) do
    redis.call('HDEL', KEYS[2], player_id)
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then
    return {0, {}, expired}
end
local existing = redis.call('HVALS', KEYS[2])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[5])
return {1, existing, expired}
"""

HEARTBEAT_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, player_id in ipairs(expired) do
    redis.call('HDEL', KEYS[2], player_id)
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local renewed = 0
if redis.call('ZSCORE', KEYS[1], ARGV[2]) then
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
    renewed = 1
end
return {expired, renewed}
"""

UPDATE_PLAYER_SCRIPT = """
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
    return 1
end
return 0
"""

RELEASE_PLAYER_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return removed
"""


def _now_ms():
    return int(time.time() * 1000)


async def _redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


async def _admit_local_player(player_id, player):
    with _local_players_lock:
        now = time.monotonic()
        players = _local_players.setdefault(ROOM_GROUP, {})
        expired = [
            existing_id for existing_id, (expires_at, _) in players.items()
            if expires_at <= now
        ]
        for existing_id in expired:
            del players[existing_id]
        if len(players) >= MAX_PLAYERS:
            return False, [], expired
        existing = [value for _, value in players.values()]
        players[player_id] = (now + PLAYER_LEASE_SECONDS, player)
        return True, existing, expired


class GameRoomConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        user = self.scope['user']
        if not user.is_authenticated:
            await self.close(code=4401)
            return

        self.player_id = uuid.uuid4().hex
        self.player = {
            'id': self.player_id,
            'name': user.get_username(),
            'x': 50,
            'y': 50,
        }
        self.joined = False
        self.last_move_at = None
        await self.channel_layer.group_add(ROOM_GROUP, self.channel_name)

        if settings.REDIS_URL:
            client = await _redis()
            now = _now_ms()
            admitted, existing, expired = await client.eval(
                ADMIT_PLAYER_SCRIPT,
                2,
                ROOM_MEMBERS_KEY,
                ROOM_PLAYERS_KEY,
                now,
                MAX_PLAYERS,
                now + PLAYER_LEASE_SECONDS * 1000,
                self.player_id,
                json.dumps(self.player),
            )
            existing = [json.loads(player) for player in existing]
        else:
            admitted, existing, expired = await _admit_local_player(
                self.player_id,
                self.player,
            )

        for player_id in expired:
            await self._broadcast_player_left(player_id)

        if not admitted:
            await self.channel_layer.group_discard(ROOM_GROUP, self.channel_name)
            await self.close(code=4429)
            return

        self.joined = True
        await self.accept()
        await self.send_json({
            'type': 'players',
            'self_id': self.player_id,
            'players': existing + [self.player],
        })
        await self.channel_layer.group_send(
            ROOM_GROUP,
            {'type': 'player.joined', 'player': self.player},
        )
        self.heartbeat_task = asyncio.create_task(self._heartbeat())

    async def disconnect(self, close_code):
        if getattr(self, 'joined', False):
            task = getattr(self, 'heartbeat_task', None)
            if task:
                task.cancel()
            if await self._release_player():
                await self._broadcast_player_left(self.player_id)
        await self.channel_layer.group_discard(ROOM_GROUP, self.channel_name)

    async def receive_json(self, content, **kwargs):
        if not isinstance(content, dict):
            await self._send_error('Expected a JSON object.')
            return

        message_type = content.get('type')
        if message_type == 'move':
            await self._move(content)
        elif message_type == 'chat':
            await self._chat(content)
        else:
            await self._send_error('Unsupported message type.')

    async def _move(self, content):
        x = content.get('x')
        y = content.get('y')
        if (
            isinstance(x, bool) or not isinstance(x, (int, float))
            or isinstance(y, bool) or not isinstance(y, (int, float))
            or not 0 <= x <= 100 or not 0 <= y <= 100
        ):
            await self._send_error('Position values must be between 0 and 100.')
            return

        now = time.monotonic()
        if (
            self.last_move_at is not None
            and now - self.last_move_at < MOVE_INTERVAL_SECONDS
        ):
            return
        self.last_move_at = now

        self.player['x'] = x
        self.player['y'] = y
        if settings.REDIS_URL:
            client = await _redis()
            expires_at = _now_ms() + PLAYER_LEASE_SECONDS * 1000
            updated = await client.eval(
                UPDATE_PLAYER_SCRIPT,
                2,
                ROOM_MEMBERS_KEY,
                ROOM_PLAYERS_KEY,
                self.player_id,
                json.dumps(self.player),
                expires_at,
            )
            if not updated:
                await self.close(code=1011)
                return
        else:
            with _local_players_lock:
                players = _local_players.get(ROOM_GROUP, {})
                active = self.player_id in players
                if active:
                    players[self.player_id] = (
                        time.monotonic() + PLAYER_LEASE_SECONDS,
                        self.player,
                    )
            if not active:
                await self.close(code=1011)
                return

        await self.channel_layer.group_send(
            ROOM_GROUP,
            {'type': 'player.moved', 'player': self.player},
        )

    async def _chat(self, content):
        message = content.get('message')
        if not isinstance(message, str):
            await self._send_error('Chat message must be text.')
            return
        message = message.strip()
        if not message or len(message) > MAX_CHAT_LENGTH:
            await self._send_error(
                f'Chat messages must be 1 to {MAX_CHAT_LENGTH} characters.'
            )
            return
        await self.channel_layer.group_send(
            ROOM_GROUP,
            {
                'type': 'chat.message',
                'name': self.player['name'],
                'message': message,
            },
        )

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            expires_at = _now_ms() + PLAYER_LEASE_SECONDS * 1000
            if settings.REDIS_URL:
                client = await _redis()
                expired, renewed = await client.eval(
                    HEARTBEAT_SCRIPT,
                    2,
                    ROOM_MEMBERS_KEY,
                    ROOM_PLAYERS_KEY,
                    _now_ms(),
                    self.player_id,
                    expires_at,
                )
            else:
                with _local_players_lock:
                    players = _local_players.get(ROOM_GROUP, {})
                    now = time.monotonic()
                    expired = [
                        player_id for player_id, (deadline, _) in players.items()
                        if deadline <= now
                    ]
                    for player_id in expired:
                        del players[player_id]
                    current = players.get(self.player_id)
                    if current is None:
                        renewed = 0
                    else:
                        players[self.player_id] = (
                            time.monotonic() + PLAYER_LEASE_SECONDS,
                            current[1],
                        )
                        renewed = 1
            for player_id in expired:
                await self._broadcast_player_left(player_id)
            if not renewed:
                await self.close(code=1011)
                return

    async def _release_player(self):
        if settings.REDIS_URL:
            client = await _redis()
            return await client.eval(
                RELEASE_PLAYER_SCRIPT,
                2,
                ROOM_MEMBERS_KEY,
                ROOM_PLAYERS_KEY,
                self.player_id,
            )
        else:
            with _local_players_lock:
                return _local_players.get(ROOM_GROUP, {}).pop(
                    self.player_id,
                    None,
                ) is not None

    async def _broadcast_player_left(self, player_id):
        await self.channel_layer.group_send(
            ROOM_GROUP,
            {'type': 'player.left', 'player_id': player_id},
        )

    async def _send_error(self, message):
        await self.send_json({'type': 'error', 'message': message})

    async def player_joined(self, event):
        if event['player']['id'] != self.player_id:
            await self.send_json({'type': 'player_joined', 'player': event['player']})

    async def player_left(self, event):
        await self.send_json({'type': 'player_left', 'player_id': event['player_id']})

    async def player_moved(self, event):
        if event['player']['id'] != self.player_id:
            await self.send_json({'type': 'player_moved', 'player': event['player']})

    async def chat_message(self, event):
        await self.send_json({
            'type': 'chat',
            'name': event['name'],
            'message': event['message'],
        })
