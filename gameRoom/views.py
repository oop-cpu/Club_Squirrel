from django.contrib.auth.decorators import login_required
from django.shortcuts import render


@login_required(login_url='login:index')
def play(request):
    return render(request, 'gameRoom/gameR.html', {})
