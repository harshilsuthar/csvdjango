from django import forms
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth.models import User


class LoginForm(AuthenticationForm):
    class Meta:
        model = User
        fields = ['username', 'password']
        
    
    
    def __init__(self, *args, **kwargs):
        super(LoginForm, self).__init__(*args, **kwargs)
        self.fields['username'].widget.attrs = {'class':'input100'}
        self.fields['password'].widget.attrs = {'class':'input100 '}
    

class RegisterForm(UserCreationForm):
    class Meta:
        model = User
        fields = ['username', 'first_name', 'last_name',
                  'email', 'password1', 'password2']

    
    def __init__(self, *args, **kwargs):
        super(RegisterForm, self).__init__(*args, **kwargs)
        self.fields['username'].widget.attrs = {'class':'input100'}
        self.fields['first_name'].widget.attrs = {'class':'input100'}
        self.fields['last_name'].widget.attrs = {'class':'input100'}
        self.fields['email'].widget.attrs = {'class':'input100'}
        self.fields['password1'].widget.attrs = {'class':'input100'}
        self.fields['password2'].widget.attrs = {'class':'input100'}