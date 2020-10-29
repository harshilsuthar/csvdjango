from django.db import models

class Auther(models.Model):
    name = models.CharField(max_length=20)
    email = models.EmailField()
    mobile = models.CharField(max_length=10)

class FileUpload(models.Model):
    file = models.FileField(upload_to='csvfiles/')


class Book(models.Model):
    name = models.CharField(max_length=20)
    auther = models.ForeignKey(Auther, on_delete= models.CASCADE)

