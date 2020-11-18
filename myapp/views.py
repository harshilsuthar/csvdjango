import csv
import ctypes
import hashlib
import inspect
import os
import queue
import threading
import time

import mysql
import mysql.connector.pooling
import pandas
import psycopg2
import psycopg2.pool
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files import File
from django.http import JsonResponse
from django.shortcuts import HttpResponse, redirect, render, reverse
from django.views import generic
from django.views.decorators.csrf import csrf_exempt

from . import models
from .forms import ConnectServerForm

# forloop_counter = 0
# total_counter = 0

mysql_pool = ''
postgres_pool = ''
data_check_time = 0
data_check_count = 0


subprocess_thread_list = []
master_thread_list = []
is_thread_manager_running = False
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
            print(port, '=====port')
            self.request.session['database_type'] = database_type
            self.request.session['username'] = username
            self.request.session['port'] = port
            self.request.session['host'] = host
            if database_type == 'postgres':
                postgres_pool = psycopg2.pool.ThreadedConnectionPool(1, 100, user=username,
                                                                     password=password,
                                                                     host=host,
                                                                     port=port,
                                                                     database="postgres")
            elif database_type == 'mysql':
                mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="mypool", pool_size=32, user=username, passwd=password, host=host, port=port)
        except psycopg2.Error as ex:
            # print(ex)
            try:
                return redirect('myapp:ConnectServer', error=ex)
            except Exception:
                return redirect('myapp:ConnectServer', error='Unknown Error')
        except mysql.connector.errors.Error as ex:
            # print(ex)
            try:
                return redirect('myapp:ConnectServer', error=ex)
            except Exception:
                return redirect('myapp:ConnectServer', error='Unknown Error')
        except Exception as ex:
            # print(ex)
            print('final exception')
            try:
                return redirect('myapp:ConnectServer', error=ex)
            except Exception:
                return redirect('myapp:ConnectServer', error='Unknown Error')
        return redirect(reverse('myapp:ListDatabaseView'))


# rendering table list into selectdatabase.html from selecttable.html
def createModel(request):
    if request.method == 'GET':
        tablelist = []
        database = request.GET.get('name')
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
                conn.close()
            elif request.session['database_type'] == 'postgres':
                conn = makeconnection(request)
                cursor = conn.cursor()
                showtables = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
                cursor.execute(showtables)
                for tables in cursor.fetchall():
                    tablelist.append(tables[1])
                cursor.close()
                conn.close()

            return render(request, 'selecttable.html', {'tables': tablelist})
        except Exception as ex:
            print(ex)
            return render(request, 'selecttable.html', {'tables': tablelist})


# database list, tablelist, csvfile GET and POST method
def listDatabaseView(request):
    try:
        # creating connection according to server

        if request.method == 'GET':
            databases = []
            try:
                conn = makeconnection(request)
                cursor = conn.cursor()

            except psycopg2.Error as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error=ex)
            except mysql.connector.errors.Error as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error=ex)
            except Exception as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error=ex)

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
            error = None
            try:
                error = request.session['ListDatabaseViewError']
                del request.session['ListDatabaseViewError']
            except Exception as ex:
                print(ex)
            finally:
                return render(request, 'selectdatabase.html', {'databases': databases, 'error': error})

        if request.method == 'POST':
            try:
                global subprocess_thread_list
                csvfile = request.FILES.get('csvfile')
                table = request.POST.get('table')
                database = request.POST.get('database')
                username = request.session['username']
                database_type = request.session['database_type']
                port = request.session['port']
                host = request.session['host']
                user = pandas.read_csv(csvfile)
                csvfile.seek(0, 0)
                if csvfile == None:
                    return JsonResponse({'error_rows': 'file not found or corrupt file!!'})
                md5_hash = hashlib.md5()
                md5_hash.update(csvfile.read())
                digest = md5_hash.hexdigest()
                error_model = models.CsvErrorFile.objects.filter(user=User.objects.get(id=1), server_name=database_type,
                                                                 server_username=username, server_port=port, server_host=host,
                                                                 server_database=database, server_table=table, uploaded_file_hash=digest,
                                                                 commited=True)
                if len(error_model) != 0:
                    request.session['ListDatabaseViewError'] = 'request is already fulfiled, check history for error data in csvfile'
                    return redirect('myapp:ListDatabaseView')
                else:
                    error_model = models.CsvErrorFile(user=User.objects.get(id=1), server_name=database_type, server_username=username,
                                                      server_port=port, server_host=host, server_database=database, server_table=table,
                                                      uploaded_file=csvfile, process_state='processing',
                                                      uploaded_file_hash=digest, commited=True)
                    error_model.save()
                    model_pk = error_model.pk

                # converting csv data for database entry
                # seperating header from csv
                header, raw_header, data = csvSplitter(user)
                # setting global counter for thread
                # global total_counter
                global master_thread_list
                global is_thread_manager_running
                # total_counter = len(data1)
                # print('totalcounter---------', total_counter)
                start_time = time.time()

                # creating csvchecksubprocess thread and append in master thread list
                commit = True
                t1 = threading.Thread(target=csvThreadCreator, args=(
                    request, database, data, table, header, raw_header, user, commit, model_pk))
                master_thread_list.insert(0, t1)
                print(len(master_thread_list),
                      'thread added in master_thread_list by listDatabaseView for commiting')

                # if thread manager is shutdown then turn it on.
                if is_thread_manager_running == False:
                    is_thread_manager_running = True
                    print('calling thread manager from listDatabaseView for commiting')
                    # master thread creator for thread manager which maintains queue.
                    t2 = threading.Thread(target=threadManager)
                    t2.start()
                print('csv check thread creation and handle time for starting background processing:',
                      time.time()-start_time)

                return redirect('myapp:ListDatabaseView')

            except Exception as ex:
                print(ex)
                return redirect('myapp:ListDatabaseView')
    except Exception as ex:
        print(ex)


# getting form using ajax try to put data inside database but not commiting.
def csvCheck(request):
    if request.method == 'POST':
        try:
            global subprocess_thread_list
            csvfile = request.FILES.get('csvfile')
            table = request.POST.get('table')
            database = request.POST.get('database')
            username = request.session['username']
            database_type = request.session['database_type']
            port = request.session['port']
            host = request.session['host']
            if csvfile == None:
                raise Exception('No file is Uploaded!!')
            user = pandas.read_csv(csvfile)
            csvfile.seek(0, 0)
            if csvfile == None:
                return JsonResponse({'error_rows': 'file not found or corrupt file!!'})
            md5_hash = hashlib.md5()
            md5_hash.update(csvfile.read())
            digest = md5_hash.hexdigest()
            error_model = models.CsvErrorFile.objects.filter(user=User.objects.get(
                id=1), server_name=database_type, server_username=username,
                server_port=port, server_host=host, server_database=database,
                server_table=table, uploaded_file_hash=digest)
            if len(error_model) != 0:
                return JsonResponse({'error_rows': 'request is already fulfilled, check history for error data file'})
            else:
                error_model = models.CsvErrorFile(user=User.objects.get(id=1), server_name=database_type,
                                                  server_username=username, server_port=port,
                                                  server_host=host, server_database=database, server_table=table,
                                                  uploaded_file=csvfile, process_state='processing',
                                                  uploaded_file_hash=digest)
                error_model.save()
                model_pk = error_model.pk

            # converting csv data for database entry
            # seperating header from csv
            header, raw_header, data = csvSplitter(user)

            # setting global counter for thread
            # global total_counter
            global master_thread_list
            global is_thread_manager_running
            # total_counter = len(data1)
            # print('totalcounter---------', total_counter)
            start_time = time.time()

            # creating csvchecksubprocess thread and append in master thread list
            commit = False
            t1 = threading.Thread(target=csvThreadCreator, args=(
                request, database, data, table, header, raw_header, user, commit, model_pk))
            master_thread_list.append(t1)
            print(len(master_thread_list),
                  'thread added in master_thread_list by csvcheck')

            if is_thread_manager_running == False:
                is_thread_manager_running = True
                print('calling thread manager from csvcheck')
                # master thread creator for thread manager which maintains queue.
                t2 = threading.Thread(target=threadManager)
                t2.start()
            print('csv check thread creation and handle time for starting background processing:',
                  time.time()-start_time)
            return JsonResponse({'error_rows': 'File Checking Under Process!!'})

        except Exception as ex:
            try:
                error_model = models.CsvErrorFile.objects.get(pk=model_pk)
                error_model.process_state = 'error'
                error_model.save()
            except Exception as ex1:
                print(
                    'error while updating model state to error inside csvThreadCreator')
                print(ex1)
            print(ex)
            return JsonResponse({'error_rows': str(ex)})


# thread management classes for csvcheck
# make a csvThreadCreator thread and wait untill it completes, called by csvCheck
# this function act as queue.
def threadManager():
    try:
        global master_thread_list
        global is_thread_manager_running
        print('thread manager is running')
        print(len(master_thread_list), 'master thread list len')
        start_time = time.time()
        while True:
            for thread in master_thread_list:
                # get thread from the list and start and wait for its execution
                thread.start()
                thread.join()
                master_thread_list.remove(thread)
                time.sleep(0.1)
            if len(master_thread_list) == 0:
                is_thread_manager_running = False
                print('stopping master thread manager')
                print('total master thread wakeup time is:',
                      time.time()-start_time)
                print(data_check_time/data_check_count)
                break
    except Exception as ex:
        print('exception in thread manager -----------------------')
        print(ex)

# create multitheaded check csv subprocess, called by threadManager, aslo create error file


def csvThreadCreator(request, database, data, table, header, raw_header, user, commit, model_pk):
    try:
        error_rows = []
        thread_count = 3
        start_time = time.time()

        subprocess_thread_list = []
        # creating multiple threads for checking csv
        for ct in range(thread_count):
            thread_value = Thread(target=lambda q, args1: q.put(csvThread(*args1)), args=(
                que, [request, database, data[ct::thread_count], table, header, ct, thread_count, commit]))
            subprocess_thread_list.append(thread_value)

        # starting all threads at a time
        for thread_value in subprocess_thread_list:
            thread_value.start()

        # joins threads one by one.
        for thread_value in subprocess_thread_list:
            thread_value.join()

        # getting sub thread return value from queue.Queue
        while not que.empty():
            error_row_chunk = que.get()
            error_rows.extend(error_row_chunk)

        # sorting error_rows and data
        print('data checking time -----------------', time.time()-start_time)
        global data_check_time
        global data_check_count
        data_check_count += 1
        data_check_time += time.time()-start_time
        error_rows.sort()

        # creating file if error list is greater than 20.
        if len(error_rows) > 0:
            start_time = time.time()
            file = 'error_file_static.csv'
            csvfile = open(file, 'w+')
            print('collecting error line')
            res_list = map(user.values.__getitem__, error_rows)
            csvfile.write(pandas.DataFrame(
                res_list, columns=raw_header).to_csv(index=False))
            print('file written!!!!!!!!')
            csvfile.seek(0, 0)

            # with open(file, 'r') as csvfile:
            error_model = models.CsvErrorFile.objects.get(pk=model_pk)
            error_model.error_file = File(csvfile)
            error_model.process_state = 'completed'
            error_model.save()
            csvfile.close()
        else:
            error_model = models.CsvErrorFile.objects.get(pk=model_pk)
            error_model.process_state = 'completed'
            error_model.save()
        print('create file time -----------------',
              time.time()-start_time)
    except Exception as ex:
        print('exception in csvThreadCreator, saving "error" in model')
        print(ex)
        try:
            error_model = models.CsvErrorFile.objects.get(pk=model_pk)
            error_model.process_state = 'error'
            error_model.save()
        except Exception as ex:
            print('error while updating model state to error inside csvThreadCreator')
            print(ex)

# its a thread process of csvthread, called by csvThreadCreator


def csvThread(request, database, data, table, header, startvalue, jumpvalue, commit):
    error_rows = []
    try:
        # creating connection to database
        conn = makeconnection(request)
        cursor = conn.cursor()
        database_type = request.session['database_type']
        if database_type == 'mysql':
            databasequery = 'USE %s' % (database)
            cursor.execute(databasequery)
        # global forloop_counter

        # checking if error occur while try to adding data into table, if error occured add into list
        for row_count, row in enumerate(data):
            try:
                # forloop_counter += 1
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    table, header, row)
                cursor.execute(insertquery)
            except Exception as ex:
                error_rows.extend([startvalue+row_count*jumpvalue])
    except Exception as ex:
        print(ex)

    finally:
        try:
            if commit == True:
                conn.commit()
            cursor.close()
            conn.close()
        except Exception as ex:
            print("error in csvThrea, can't close cursor or conn")
            print(ex)
        finally:
            print(len(error_rows), '-------finally')
            return error_rows
# thread management ends here


def csvSplitter(user):
    data = []
    raw_header = tuple(user.head())
    header = str(raw_header)
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
    return None, None, None
# table schema view


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
# def getCurrentProcessCount(request):
#     if request.method == 'GET':
#         return JsonResponse({'current_counter': forloop_counter, 'total_counter': total_counter})
