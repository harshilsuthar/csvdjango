from django.urls import path, reverse_lazy
from django.views.generic import RedirectView
from . import views

app_name = 'myapp'

urlpatterns = [
    path('', RedirectView.as_view(url=reverse_lazy('myapp:ConnectServer')),name='ConnectServerRedirect'),
    path('connectserver/', views.DatabaseConfigView.as_view(), name='ConnectServer'),
    path('connectserver/<error>', views.DatabaseConfigView.as_view(), name='ConnectServer'),
    path('listdatabase/', views.listDatabaseView,kwargs={}, name='ListDatabaseView'),
    path('createmodel/',views.createModel, name='CreateModel'),
    path('showtablecolumns/',views.showTableColumns, name='ShowTableColumns'),
    path('csvcheck/', views.csvCheck, name='CsvCheck'),
    path('responseCsvHeader/', views.responseCsvHeader, name='ResponseCsvHeader'),
    # path('getcurrentprocesscount/',views.getCurrentProcessCount,name='GetCurrentProcessCount'),
] 
