from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
from .forms import LoginForm, RegisterForm
# from myapp.views import 
# Create your views here.

def loginView(request):
    if request.method == 'GET':
        return render(request, 'login.html', {'form':LoginForm})
    if request.method == 'POST':
        form = LoginForm(data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(username=username, password=password)
            if user:
                login(request, user)
                return redirect('myapp:ConnectServer')
            else:
                return redirect('nevigate:Login')
        else:
            return render(request, 'login.html', {'form':form})


def registerView(request):
    if request.method == 'GET':
        form = RegisterForm()
        return render(request, 'register.html',{'form':form})
    
    if request.method == 'POST':
        form = RegisterForm(data=request.POST)
        if form.is_valid():
            print('registe form validated')
            form.save()
            return redirect('nevigate:Login')
        else:
            return render(request, 'register.html',{'form':form})
    
    pass

def logoutView(request):
    try:
        logout(request)
        return redirect('nevigate:Login')
    except Exception as ex:
        print(ex)
        return redirect('nevigate:Login')