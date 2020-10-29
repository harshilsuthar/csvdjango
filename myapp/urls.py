from django.urls import path
from django.urls import reverse_lazy
from django.views.generic import RedirectView
from . import views

urlpatterns = [
    path('', RedirectView.as_view(url=reverse_lazy('ConnectServer'))),
    path('connectserver/', views.DatabaseConfigView.as_view(), name='ConnectServer'),
    path('connectserver/<error>', views.DatabaseConfigView.as_view(), name='ConnectServer'),
    path('listdatabase/', views.listDatabaseView,kwargs={}, name='ListDatabaseView'),
    path('createmodel/',views.createModel, name='CreateModel'),
   
]
