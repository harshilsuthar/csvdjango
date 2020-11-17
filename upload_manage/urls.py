from django.urls import path
from . import views

app_name = 'upload_manage'
urlpatterns = [
    path('',views.history, name='History')
]