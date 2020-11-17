from django.shortcuts import render
from django.contrib.auth import login, logout, authenticate
# from myapp.views import 
# Create your views here.

def loginview(request):
    if request.method == 'GET':
        return render(request, 'login.html')
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(username, password)
        if user:
            login(request, user)
        else:
            return redirect('login')

def logout(request):
    pass