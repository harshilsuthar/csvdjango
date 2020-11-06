from django.db import models
from django.contrib.auth.models import User

class CsvErrorFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process_state = models.CharField(max_length=20,choices=(('running','Running'),('completed','Completed'),('error','Error')))
    error_file = models.FileField(upload_to='error_files')
    