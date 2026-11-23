import time
from types import SimpleNamespace

from channels.testing import WebsocketCommunicator
from django.contrib.auth.models import AnonymousUser
from django.test import SimpleTestCase, override_settings
from django.test import RequestFactory

from .consumers import (
    MAX_PLAYERS,
    ROOM_GROUP,
    GameRoomConsumer,
    _admit_local_player,
    _local_players,
    _local_players_lock,
)
from .views import play


def authenticated_room_app(username):
    consumer = GameRoomConsumer.as_asgi()

    async def app(scope, receive, send):
        scope['user'] = SimpleNamespace(
            is_authenticated=True,
            get_username=lambda: username,
        )
        await consumer(scope, receive, send)

    return app


@override_settings(REDIS_URL=None)
class RoomCapacityTests(SimpleTestCase):
    async def _reset_local_players(self):
        with _local_players_lock:
            _local_players.pop(ROOM_GROUP, None)

    async def test_room_rejects_players_over_limit(self):
        await self._reset_local_players()
        for player_number in range(MAX_PLAYERS):
            player = {'id': str(player_number), 'name': f'player-{player_number}'}
            admitted, _, _ = await _admit_local_player(player['id'], player)
            self.assertTrue(admitted)

        admitted, _, _ = await _admit_local_player('sixth-player', {
            'id': 'sixth-player',
            'name': 'player-6',
        })

        self.assertFalse(admitted)

    async def test_sixth_connection_is_rejected(self):
        await self._reset_local_players()
        communicators = []
        try:
            for player_number in range(MAX_PLAYERS):
                communicator = WebsocketCommunicator(
                    authenticated_room_app(f'player-{player_number}'),
                    '/ws/room/',
                )
                connected, _ = await communicator.connect()
                self.assertTrue(connected)
                await communicator.receive_json_from()
                communicators.append(communicator)

            sixth = WebsocketCommunicator(
                authenticated_room_app('player-6'),
                '/ws/room/',
            )
            connected, close_code = await sixth.connect()
            self.assertFalse(connected)
            self.assertEqual(close_code, 4429)
        finally:
            for communicator in communicators:
                await communicator.disconnect()


@override_settings(REDIS_URL=None)
class RoomMessageTests(SimpleTestCase):
    async def _reset_local_players(self):
        with _local_players_lock:
            _local_players.pop(ROOM_GROUP, None)

    async def test_players_receive_movement_and_chat(self):
        await self._reset_local_players()
        first = WebsocketCommunicator(
            authenticated_room_app('first'),
            '/ws/room/',
        )
        second = WebsocketCommunicator(
            authenticated_room_app('second'),
            '/ws/room/',
        )
        try:
            self.assertTrue((await first.connect())[0])
            first_initial = await first.receive_json_from()
            self.assertTrue((await second.connect())[0])
            second_initial = await second.receive_json_from()
            first_join = await first.receive_json_from()
            self.assertEqual(first_initial['self_id'], first_initial['players'][-1]['id'])
            self.assertEqual(second_initial['self_id'], second_initial['players'][-1]['id'])
            self.assertEqual(first_join['type'], 'player_joined')

            await first.send_json_to({'type': 'move', 'x': 25, 'y': 70})
            movement = await second.receive_json_from()
            self.assertEqual(movement['type'], 'player_moved')
            self.assertEqual((movement['player']['x'], movement['player']['y']), (25, 70))

            await first.send_json_to({'type': 'chat', 'message': 'Hello!'})
            self.assertEqual((await first.receive_json_from())['type'], 'chat')
            chat = await second.receive_json_from()
            self.assertEqual(chat['type'], 'chat')
            self.assertEqual(chat['message'], 'Hello!')

            await first.disconnect()
            departure = await second.receive_json_from()
            self.assertEqual(departure, {
                'type': 'player_left',
                'player_id': first_initial['self_id'],
            })
        finally:
            await second.disconnect()


@override_settings(REDIS_URL=None)
class RoomPresenceTests(SimpleTestCase):
    async def _reset_local_players(self):
        with _local_players_lock:
            _local_players.pop(ROOM_GROUP, None)

    async def test_expired_player_is_removed_from_other_clients(self):
        await self._reset_local_players()
        first = WebsocketCommunicator(
            authenticated_room_app('first'),
            '/ws/room/',
        )
        second = WebsocketCommunicator(
            authenticated_room_app('second'),
            '/ws/room/',
        )
        third = WebsocketCommunicator(
            authenticated_room_app('third'),
            '/ws/room/',
        )
        communicators = (first, second, third)
        try:
            self.assertTrue((await first.connect())[0])
            first_initial = await first.receive_json_from()
            self.assertTrue((await second.connect())[0])
            await second.receive_json_from()
            await first.receive_json_from()

            with _local_players_lock:
                _local_players[ROOM_GROUP][first_initial['self_id']] = (
                    time.monotonic() - 1,
                    {'id': first_initial['self_id'], 'name': 'first'},
                )

            self.assertTrue((await third.connect())[0])
            await third.receive_json_from()
            departure = await second.receive_json_from()
            self.assertEqual(departure, {
                'type': 'player_left',
                'player_id': first_initial['self_id'],
            })
        finally:
            for communicator in communicators:
                await communicator.disconnect()


class GameRoomViewTests(SimpleTestCase):
    def test_room_redirects_anonymous_users_to_login(self):
        request = RequestFactory().get('/play/')
        request.user = AnonymousUser()

        response = play(request)

        self.assertEqual(response.status_code, 302)
        self.assertIn('/login/', response.url)
