from django.shortcuts import render
from myapp.models import CsvErrorFile
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
# Create your views here.
@login_required
def history(request):
    user1=User.objects.get(id=request.user.id)
    # user = request.user
    error_model = CsvErrorFile.objects.filter(user=user1).order_by('-upload_time')
    return render(request, 'history.html', {'history_data':error_model})