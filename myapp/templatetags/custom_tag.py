from django import template
from django.shortcuts import HttpResponse

register = template.Library()

@register.filter(name='csv_checker')
def csv_checker(main_list, header_data):
    first_value_list = []
    ls = []
    for row in main_list:
        first_value_list.append(row[0])
    for data in header_data:
        if data not in first_value_list:
            ls.append(data)
        else:
            ls.append('')
    return ls