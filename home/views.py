from django.shortcuts import render

def index(request):
    return render(request, 'home/index.html', {})

def settings_page(request):
    return render(request, 'home/settings.html')