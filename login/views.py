from django.contrib.auth.views import LoginView
from django.contrib.auth.forms import UserCreationForm
from django.shortcuts import render, redirect
from django.contrib.auth import login


class LoginPageView(LoginView):
    template_name = 'login/login.html'
    next_page = 'home:index'


def register(request):
    if request.method == 'POST':
        form = UserCreationForm(request.POST)

        if form.is_valid():
            user = form.save()
            login(request, user)
            print("==============")
            print(user)
            print(user.id)
            print(user.username)
            print(user.password)
            print("==============")
            return redirect('home:index')
    else:
        form = UserCreationForm()

    return render(request, 'login/register.html', {
        'form': form
    })
