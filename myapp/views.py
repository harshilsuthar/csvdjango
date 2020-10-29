import dependency_injector
import mysql
import mysql.connector.pooling
import os
import pandas
import psycopg2
from django.shortcuts import HttpResponse, redirect, render, reverse
from django.views import generic
from psycopg2 import pool

from . import models
from .forms import ConnectServerForm

mysql_pool = ''
postgres_pool = ''


class DatabaseConfigView(generic.FormView):
    form_class = ConnectServerForm
    template_name = 'connectserver.html'
    success_url = '/'

    def get_context_data(self, **kwargs):
        try:
            context = super().get_context_data(**kwargs)
            context['error'] = self.kwargs['error']
            return context
        except:
            context['error'] = ''
            return context

    def form_valid(self, form):
        try:
            database_type = form.cleaned_data['database_type']
            username = form.cleaned_data['username']
            password = form.cleaned_data['password']
            host = form.cleaned_data['host']
            port = form.cleaned_data['port']
            self.request.session['database_type'] = database_type
            self.request.session['username'] = username
            self.request.session['password'] = password
            self.request.session['host'] = host
            self.request.session['port'] = port
            if database_type == 'postgres':
                global postgres_pool
                postgres_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, user=username,
                                                                     password=password,
                                                                     host=host,
                                                                     port=port,
                                                                     database="postgres")
            elif database_type == 'mysql':
                global mysql_pool
                mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="mypool", pool_size=10, user=username, passwd=password, host=host, port=port)

        except Exception as ex:
            print(ex)
            return redirect('/connectserver/')
        return redirect(reverse('ListDatabaseView'))

# rendering table list into selectdatabase.html from selecttable.html


def createModel(request):
    if request.method == 'GET':
        tablelist = []
        database = request.GET.get('name')
        username = request.session['username']
        password = request.session['password']
        host = request.session['host']
        port = request.session['port']
        try:
            if request.session['database_type'] == 'mysql':
                conn = mysql_pool.get_connection()
                cursor = conn.cursor()
                usedatabase = 'USE %s' % (database)
                cursor.execute(usedatabase)
                showtables = 'show tables;'
                cursor.execute(showtables)
                for tables in cursor.fetchall():
                    tables = str(tuple(tables))
                    tables = tables[2:-3]
                    tablelist.append(tables)
                cursor.close()
                conn.commit()
                conn.close()
            elif request.session['database_type'] == 'postgres':
                conn = postgres_pool.getconn()
                cursor = conn.cursor()
                showtables = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
                cursor.execute(showtables)
                for tables in cursor.fetchall():
                    tablelist.append(tables[1])
                cursor.close()
                conn.commit()
                conn.close()

            return render(request, 'selecttable.html', {'tables': tablelist})
        except Exception as ex:
            print(ex)
            return render(request, 'selecttable.html', {'tables': tablelist})


def listDatabaseView(request):
    username = request.session['username']
    password = request.session['password']
    host = request.session['host']
    port = request.session['port']

    # creating connection according to server
    try:
        if request.session['database_type'] == 'mysql':
            conn = mysql_pool.get_connection()
            cursor = conn.cursor()

        elif request.session['database_type'] == 'postgres':
            conn = postgres_pool.getconn()
            cursor = conn.cursor()

    except psycopg2.Error as ex:
        print(ex)
        return redirect('ConnectServer', error=ex)
    except mysql.connector.Error as ex:
        print(ex)
        return redirect('ConnectServer', error=ex)
    except Exception as ex:
        print(ex)
        return redirect('/')

    if request.method == 'GET':
        databases = []

        # executing show database query according to server
        if request.session['database_type'] == 'postgres':
            cursor.execute(
                'SELECT datname FROM pg_database WHERE datistemplate = false;')
        elif request.session['database_type'] == 'mysql':
            cursor.execute('SHOW DATABASES')
        for database in cursor.fetchall():
            databases.append(database[0])
        request.session['user'+'database'] = databases

        # rendering database list
        return render(request, 'selectdatabase.html', {'databases': databases})

    if request.method == 'POST':
        # getting post data
        database = request.POST.get('database')
        table = request.POST.get('table')
        csvfile = request.FILES.get('csvfile')

        # changing selected table string view according server
        posttable = table
        try:
            # adding csv into database
            fileobject = models.FileUpload.objects.create(file=csvfile)
            user = pandas.read_csv('media/'+fileobject.file.name)
            data = []

            # converting csv data for database entry
            # seperating header from csv
            header = str(tuple(user.head()))
            if len(tuple(user.head())) == 1:
                headindex = header.rfind(',')
                header = header[:headindex]+''+header[headindex+1:]
                header = header.replace("'", "")
                print(header)
            else:
                header = header.replace("'", "")

            # seperating values from csv
            for row in user.values:
                if len(tuple(row)) == 1:
                    print('--------', tuple(row))
                    row = str(tuple(row))
                    print(row)
                    rowindex = row.rfind(',')
                    row = row[:rowindex]+''+row[rowindex+1:]
                    data.append(row)
                else:
                    row = str(tuple(row))
                    data.append(row)
            data = ','.join(data)
            models.FileUpload.objects.filter(file=fileobject.file.name).first().delete()
            os.remove('media/'+fileobject.file.name)
            print('--------------deleted')
        except Exception as ex:
            print(ex)
            return render(request, 'selectdatabase.html', {'databases': request.session['user'+'database'], 'error': 'Error! there is a problem while extracting the file'})
        print(data)

        # inserting csv data into database
        try:
            # inserting data according to mysql
            if request.session['database_type'] == 'mysql':
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    table, header, data)
                print(insertquery)
                databasequery = 'USE %s' % (database)
                cursor.execute(databasequery)
                cursor.execute(insertquery)
                select_query = 'select * from %s' % (table)
                cursor.execute(select_query)
                alldata = cursor.fetchall()
                cursor.close()
                conn.commit()
                conn.close()
                print('--------------ok')

            # inserting data according to postgres
            elif request.session['database_type'] == 'postgres':
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    posttable, header, data)
                print(insertquery)
                cursor.execute(insertquery)
                select_query = 'select * from %s' % (posttable)
                cursor.execute(select_query)
                alldata = cursor.fetchall()
                cursor.close()
                conn.commit()
                conn.close()
        except Exception as ex:
            print(ex)
            return render(request, 'selectdatabase.html', {'databases': request.session['user'+'database'], 'error': 'Error! could not enter data into database\n please match columns and datatype with table'})

        return HttpResponse('data added'+'\n'+str(alldata)+"""<a href='/listdatabase/'>Add Another Data</a>""")
