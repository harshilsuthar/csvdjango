from django.urls import path
from .views import loginView, logoutView, registerView


app_name = 'nevigate'
urlpatterns = [
    path('login/',loginView, name='Login'),
    path('register/',registerView, name='Register'),
    path('logout/',logoutView, name='Logout'),
]
