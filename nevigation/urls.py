from django.urls import path
from .views import loginView, logoutView


app_name = 'nevigate'
urlpatterns = [
    path('login/',loginView, name='Login'),
    path('logout/',logoutView, name='Logout'),
]
