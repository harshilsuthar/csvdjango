from django import forms


class ConnectServerForm(forms.Form):
    database_type = forms.ChoiceField(choices=(('mysql', 'MySql'),('postgres', 'PostgreSQL')), widget=forms.Select(attrs={'class':'form-control'}))
    host = forms.CharField(max_length=50, initial='localhost',widget=forms.TextInput(attrs={'class':'form-control'}))
    port = forms.IntegerField(max_value=100000, min_value=0, initial=3306,widget=forms.TextInput(attrs={'class':'form-control'}))
    username = forms.CharField(max_length=30,widget=forms.TextInput(attrs={'class':'form-control'}))
    password = forms.CharField(max_length=30, widget=forms.PasswordInput(attrs={'class':'form-control'}))
    

    field_order = ['database_type','host','port','username','password']