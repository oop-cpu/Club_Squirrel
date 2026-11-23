from django.urls import path

from .consumers import GameRoomConsumer

websocket_urlpatterns = [
    path('ws/room/', GameRoomConsumer.as_asgi()),
]
