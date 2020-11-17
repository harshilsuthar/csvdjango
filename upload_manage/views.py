from django.shortcuts import render
from myapp.models import CsvErrorFile
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
# Create your views here.
# @login_required
def history(request):
    user1=User.objects.get(id=1)
    # user = request.user
    error_model = CsvErrorFile.objects.filter(user=user1)
    return render(request, 'history.html', {'history_data':error_model})