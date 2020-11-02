import dependency_injector
import mysql
import mysql.connector.pooling
import os
import pandas
import psycopg2
import psycopg2.pool

from django.shortcuts import HttpResponse, redirect, render, reverse
from django.views import generic
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse

from . import models
from .forms import ConnectServerForm

mysql_pool = ''
postgres_pool = ''

# making connection pool to database


def makeconnection(request):
    if request.session['database_type'] == 'mysql':
        conn = mysql_pool.get_connection()
    elif request.session['database_type'] == 'postgres':
        conn = postgres_pool.getconn()
    return conn

# server select view


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
            return redirect('ConnectServer', error=ex)
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
                conn = makeconnection(request)
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
                conn = makeconnection(request)
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

    if request.method == 'GET':
        databases = []
        try:
            conn = makeconnection(request)
            cursor = conn.cursor()

        except psycopg2.Error as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)
        except mysql.connector.errors.Error as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)
        except Exception as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)

        # executing show database query according to server
        if request.session['database_type'] == 'postgres':
            cursor.execute(
                'SELECT datname FROM pg_database WHERE datistemplate = false;')
        elif request.session['database_type'] == 'mysql':
            cursor.execute('SHOW DATABASES')
        for database in cursor.fetchall():
            databases.append(database[0])
        request.session['userdatabase'] = databases
        cursor.close()
        conn.close()
        # rendering database list
        return render(request, 'selectdatabase.html', {'databases': databases})

    if request.method == 'POST':
        try:
            conn = makeconnection(request)
            cursor = conn.cursor()
        except psycopg2.Error as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)
        except mysql.connector.errors.Error as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)
        except Exception as ex:
            print(ex)
            return redirect('ConnectServer', error=ex)
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

            else:
                header = header.replace("'", "")

            # seperating values from csv
            for row in user.values:
                if len(tuple(row)) == 1:
                    row = str(tuple(row))
                    rowindex = row.rfind(',')
                    row = row[:rowindex]+''+row[rowindex+1:]
                    data.append(row)
                else:
                    row = str(tuple(row))
                    data.append(row)
            data1 = data
            data = ','.join(data)
            models.FileUpload.objects.filter(
                file=fileobject.file.name).first().delete()
            os.remove('media/'+fileobject.file.name)
        except Exception as ex:
            print(ex)
            cursor.close()
            conn.close()
            return render(request, 'selectdatabase.html', {'databases': request.session['userdatabase'], 'error': 'Error! there is a problem while extracting the file'})

        # inserting csv data into database
        try:
            # inserting data according to mysql
            if request.session['database_type'] == 'mysql':
                databasequery = 'USE %s' % (database)
                cursor.execute(databasequery)
                error_rows = []
                print(data1)
                for row_count, row in enumerate(data1):
                    try:
                        print(data)
                        insertquery = "INSERT INTO %s %s VALUES %s;" % (
                            table, header, row)
                        cursor.execute(insertquery)
                    except Exception as ex:
                        error_rows.append([row_count, row])
                if len(error_rows) != 0:
                    cursor.close()
                    conn.close()
                    return render(request, 'selectdatabase.html', {'error_rows': error_rows, 'databases': request.session['userdatabase']})
                select_query = 'select * from %s' % (table)
                cursor.execute(select_query)
                alldata = cursor.fetchall()
                cursor.close()
                conn.commit()
                conn.close()

            # inserting data according to postgres
            elif request.session['database_type'] == 'postgres':
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    posttable, header, data)
                cursor.execute(insertquery)
                select_query = 'select * from %s' % (posttable)
                cursor.execute(select_query)
                alldata = cursor.fetchall()
                cursor.close()
                conn.commit()
                conn.close()
        except Exception as ex:
            print(ex)
            cursor.close()
            conn.close()
            return render(request, 'selectdatabase.html', {'databases': request.session['userdatabase'], 'error': 'Error! could not enter data into database\n please match columns and datatype with table'})

        return HttpResponse('data added'+'\n'+str(alldata)+"""<a href='/listdatabase/'>Add Another Data</a>""")


def showTableColumns(request):

    if request.method == 'GET':
        try:
            conn = makeconnection(request)
            cursor = conn.cursor()
            tablename = request.GET.get('tablename')
            database = request.GET.get('database')
            database_type = request.session['database_type']
            if database_type == 'mysql':
                select_query = "SHOW COLUMNS FROM %s;" % (tablename)
                use_query = 'USE %s' % (database)
                cursor.execute(use_query)
            elif database_type == 'postgres':
                print('-----inside ')
                select_query = "SELECT column_name,data_type  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';" % (
                    tablename)
            cursor.execute(select_query)
            column_list = []
            for row in cursor.fetchall():
                print('----', row)
                column_list.append(row)
            cursor.close()
            conn.close()
            if database_type == 'mysql':
                return render(request, 'tablecolumnsql.html', {'columns': column_list})
            elif database_type == 'postgres':
                return render(request, 'tablecolumnpostgres.html', {'columns': column_list})
        except Exception as ex:
            if database_type == 'mysql':
                return render(request, 'tablecolumnsql.html', {'columns': ''})
            elif database_type == 'postgres':
                return render(request, 'tablecolumnpostgres.html', {'columns': ''})


def csvCheck(request):
    if request.method == 'POST':
        try:
            # getting data from selectdatabase.html using ajax
            csvfile = request.FILES.get('csvfile')
            table = request.POST.get('table')
            database = request.POST.get('database')
            print(csvfile)
            if csvfile == None:
                raise Exception('file not found or corrupt file!!')
            data = []

            # converting csv data for database entry
            user = pandas.read_csv(csvfile)

            # seperating header from csv
            header = str(tuple(user.head()))
            if len(tuple(user.head())) == 1:
                headindex = header.rfind(',')
                header = header[:headindex]+''+header[headindex+1:]
                header = header.replace("'", "")
            else:
                header = header.replace("'", "")

            # seperating values from csv
            for row in user.values:
                if len(tuple(row)) == 1:
                    row = str(tuple(row))
                    rowindex = row.rfind(',')
                    row = row[:rowindex]+''+row[rowindex+1:]
                    data.append(row)
                else:
                    row = str(tuple(row))
                    data.append(row)
            data1 = data

            # creating connection mysql
            conn = mysql_pool.get_connection()
            cursor = conn.cursor()

            # using databse query fire
            databasequery = 'USE %s' % (database)
            cursor.execute(databasequery)

            # checking if error occur while try to adding data into table, if error occured add into list
            error_rows = []
            for row_count, row in enumerate(data1):
                try:
                    print(data)
                    insertquery = "INSERT INTO %s %s VALUES %s;" % (
                        table, header, row)
                    cursor.execute(insertquery)
                except Exception as ex:
                    error_rows.append([row_count, row])
            if len(error_rows) != 0:
                cursor.close()
                conn.close()
                return JsonResponse({'error_rows': error_rows})
            else:
                cursor.close()
                conn.close()
                return JsonResponse({'error_rows': ''})

        except Exception as ex:
            return JsonResponse({'error_rows': 'File Not Found!!'})
