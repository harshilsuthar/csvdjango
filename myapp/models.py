from django.db import models
from django.contrib.auth.models import User

class CsvErrorFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    server_name = models.CharField(max_length=20,choices=(('mysql','MySQL'),('postgres','PostgreSQL')),blank=True)
    server_username = models.CharField(max_length=30, blank=True)
    server_port = models.IntegerField(models.PositiveIntegerField,blank=True)
    server_host = models.CharField(max_length=100,blank=True)
    server_database = models.CharField(max_length=50,blank=True)
    server_table = models.CharField(max_length=50,blank=True)
    process_state = models.CharField(max_length=20,choices=(('processing','Processing'),('completed','Completed'),('error','Error')),blank=True)
    uploaded_file = models.FileField(upload_to='uploads', blank=True)
    uploaded_file_hash = models.CharField(max_length=32, blank=True)
    error_file = models.FileField(upload_to='error_files', blank=True)
    commited = models.BooleanField(default=False, blank=True)
    upload_time = models.DateTimeField(auto_now_add=True)
    finish_time = models.DateTimeField(auto_now=True)
