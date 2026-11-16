from django.urls import path
from django.contrib.auth.views import LogoutView
from .views import LoginPageView, register

app_name = 'login'

urlpatterns = [
    path('', LoginPageView.as_view(), name='index'),
    path('register/', register, name='register'),
    path('logout/', LogoutView.as_view(next_page='home:index'), name='logout'),
]