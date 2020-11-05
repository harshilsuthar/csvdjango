import dependency_injector
import mysql

import mysql.connector.pooling
import os
import pandas
import psycopg2
import psycopg2.pool
import threading
import time
import csv
import queue
import inspect
import ctypes

from django.shortcuts import HttpResponse, redirect, render, reverse
from django.views import generic
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.core.cache import cache
from multiprocessing import Process

from . import models
from .forms import ConnectServerForm


forloop_counter = 0
total_counter = 0

mysql_pool = ''
postgres_pool = ''

thread_list = []
que = queue.Queue()


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class Thread(threading.Thread):
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.is_alive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        _async_raise(self._get_my_tid(), exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should 
        cause the thread to exit silently (unless caught)"""
        self.raise_exc(SystemExit)


# making connection pool to database
def makeconnection(request):
    try:
        if request.session['database_type'] == 'mysql':
            conn = mysql_pool.get_connection()
        elif request.session['database_type'] == 'postgres':
            conn = postgres_pool.getconn()
        return conn
    except Exception as ex:
        return None


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
            global mysql_pool
            global postgres_pool
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
                postgres_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, user=username,
                                                                     password=password,
                                                                     host=host,
                                                                     port=port,
                                                                     database="postgres")
            elif database_type == 'mysql':
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

# database list, tablelist, csvfile GET and POST method


def listDatabaseView(request):
    try:
        username = request.session['username']
        password = request.session['password']
        host = request.session['host']
        port = request.session['port']
        global thread_list
        global c
        try:
            if len(thread_list) != 0:
                start_time = time.time()
                for thread_value in thread_list:
                    thread_value.terminate()
                    thread_value.join()

                while not que.empty():
                    que.get()
                    q.task_done()

                thread_list = []
                print('restart thread------------', time.time()-start_time)
        except Exception as ex:
            print(ex)

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
                user = pandas.read_csv(csvfile)
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
                    for row_count, row in enumerate(data1):
                        try:
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
    except Exception as ex:
        print(ex)
        return render(request, 'selectdatabase.html', {'databases': request.session['userdatabase'], 'error': 'Error! could not enter data into database\n please match columns and datatype with table'})
# getting form using ajax try to put data inside database but not commiting.


def csvCheck(request):
    if request.method == 'POST':
        try:
            global thread_list
            csvfile = request.FILES.get('csvfile')
            table = request.POST.get('table')
            database = request.POST.get('database')
            if csvfile == None:
                raise Exception('file not found or corrupt file!!')
            data = []

            # converting csv data for database entry
            user = pandas.read_csv(csvfile)

            # seperating header from csv
            raw_header = tuple(user.head())
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
            error_rows = []
            error_rows_index = []

            # setting global counter for thread
            global total_counter
            global forloop_counter
            forloop_counter = 0
            total_counter = len(data1)
            print('totalcounter---------', total_counter)

            #starting thread for csv checking

            # thread management
            thread_count = 3
            start_time = time.time()

            thread_list = []
            for ct in range(thread_count):
                # thread_value = ThreadWithReturnValue(target=c.checkCsvSubProcess, args=(
                #     request, database, data1[ct::thread_count], table, header, ct, thread_count, lambda: stop_threads))
                thread_value = Thread(target=lambda q, args1: q.put(checkCsvSubProcess(*args1)), args=(
                    que, [request, database, data1[ct::thread_count], table, header, ct, thread_count]))
                thread_list.append(thread_value)
            for thread_value in thread_list:
                thread_value.start()
            for thread_value in thread_list:
                thread_value.join()

            while not que.empty():
                error_row_chunk, error_row_index_chunk = que.get()
                error_rows_index.extend(error_row_index_chunk)
                error_rows.extend(error_row_chunk)
            #thread management ends

            print('data checking time -----------------', time.time()-start_time)
            error_rows.sort()
            error_rows_index.sort()

            # creating file if error list is greater than 20.
            file_bool = False
            if len(error_rows) > 20:
                start_time = time.time()
                file = 'media/csvfiles/error_file_static.csv'
                csvfile = open(file, 'w')
                # csvwriter = csv.writer(csvfile)
                # csvwriter.writerow(raw_header)
                print('collecting error line')
                # getting index from error_row list and fetching same record from user uploaded file and creating new file with fetched record
                user_list = user.values.tolist()

                error_files = list(
                    map(user_list.__getitem__, error_rows_index))
                df = pandas.DataFrame(error_files, columns=raw_header)

                df.to_csv(file)
                print('error file written!!!!!!!!')
                csvfile.close()
                file_bool = True
                print('create file time -----------------',
                      time.time()-start_time)

                try:
                    j_response = JsonResponse(
                        {'error_rows': error_rows, 'file_url': file, 'file_bool': file_bool})
                    return j_response
                except Exception as ex:
                    print(ex)

            elif len(error_rows) != 0:
                j_response = JsonResponse(
                    {'error_rows': error_rows, 'file_bool': file_bool})
                return j_response
            else:
                return JsonResponse({'error_rows': ''})

        except Exception as ex:
            return JsonResponse({'error_rows': 'File Not Found!!'})

# thread view for csvcheck


def checkCsvSubProcess(request, database, data1, table, header, startvalue, jumpvalue):
    error_rows = []
    error_row_index = []
    try:
        # creating connection to database
        conn = makeconnection(request)
        cursor = conn.cursor()
        database_type = request.session['database_type']
        if database_type == 'mysql':
            databasequery = 'USE %s' % (database)
            cursor.execute(databasequery)
        global forloop_counter

        # checking if error occur while try to adding data into table, if error occured add into list
        for row_count, row in enumerate(data1):
            try:
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    table, header, row)
                cursor.execute(insertquery)
            except Exception as ex:
                forloop_counter += 1
                error_rows.append([startvalue+row_count*jumpvalue, row])
                error_row_index.append(startvalue+row_count*jumpvalue)
    except Exception as ex:
        print(ex)

    finally:
        cursor.close()
        conn.close()
        print(len(error_rows), len(error_row_index), '-------finally')
        return error_rows, error_row_index


# table schema view

def canclecsvcheck(request):
    pass

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
                select_query = "SELECT column_name,data_type  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';" % (
                    tablename)
            cursor.execute(select_query)
            column_list = []
            for row in cursor.fetchall():
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

#increase progressbar view


def getCurrentProcessCount(request):
    if request.method == 'GET':
        return JsonResponse({'current_counter': forloop_counter, 'total_counter': total_counter})
