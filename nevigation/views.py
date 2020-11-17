from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
# from myapp.views import 
# Create your views here.

def loginView(request):
    if request.method == 'GET':
        return render(request, 'login.html')
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(username=username, password=password)
        if user:
            login(request, user)
            return redirect('myapp:ConnectServer')
        else:
            return redirect('nevigate:Login')


def logoutView(request):
    try:
        logout(request)
        return redirect('nevigate:Login')
    except Exception as ex:
        print(ex)
        return redirect('nevigate:Login')