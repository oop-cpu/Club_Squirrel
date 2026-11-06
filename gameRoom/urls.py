from django.urls import path
from . import views

app_name = 'gameRoom'

urlpatterns = [
    path('', views.play, name='gameR'),
]
