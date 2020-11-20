from django.shortcuts import render
from myapp.models import CsvErrorFile
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import DeleteView
from django.urls import reverse_lazy
# Create your views here.
@login_required
def history(request):
    user1=User.objects.get(id=request.user.id)
    # user = request.user
    error_model = CsvErrorFile.objects.filter(user=user1).order_by('-upload_time')
    return render(request, 'history.html', {'history_data':error_model})


class DeleteHistory(LoginRequiredMixin,DeleteView):
    model = CsvErrorFile
    success_url = reverse_lazy('upload_manage:History')
    def get(self, request, *args, **kwargs):
        return self.post(request, *args, **kwargs)