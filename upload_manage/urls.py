from django.urls import path
from . import views

app_name = 'upload_manage'
urlpatterns = [
    path('',views.history, name='History'),
    path('delete_history/<int:pk>',views.DeleteHistory.as_view(), name='DeleteHistory')
]