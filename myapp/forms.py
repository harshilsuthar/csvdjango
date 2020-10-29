from django import forms


class ConnectServerForm(forms.Form):
    database_type = forms.ChoiceField(choices=(('mysql', 'MySql'),('postgres', 'PostgreSQL')))
    username = forms.CharField(max_length=30)
    password = forms.CharField(max_length=30, widget=forms.PasswordInput)
    host = forms.CharField(max_length=50, initial='localhost')
    port = forms.IntegerField(max_value=100000, min_value=0, initial=3306)
