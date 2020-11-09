from django.shortcuts import render
from myapp.models import CsvErrorFile
from django.contrib.auth.models import User
# Create your views here.

def history(request):
    error_model = CsvErrorFile.objects.filter(user=User.objects.get(id=1))
    return render(request, 'history.html', {'history_data':error_model})