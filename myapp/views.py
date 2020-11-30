import ctypes
import hashlib
import inspect
import queue
import threading
import time

import mysql
import mysql.connector.pooling
import pandas
import psycopg2
import psycopg2.pool
from django.contrib.auth.models import User
from django.core.files import File
from django.http import JsonResponse
from django.shortcuts import redirect, render, reverse
from django.views import generic
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth import logout

from . import models
from .forms import ConnectServerForm

# mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
#                     pool_name="mypool", pool_size=32, user='root', passwd='root', host='localhost', port=3306)
postgres_pool = ''
mysql_pool = ''
data_check_time = 0
data_check_count = 0


subprocess_thread_list = []
parallel_user_thread_dict = dict()
is_thread_manager_running = False
que = queue.Queue()
#make thread returnable and terminable


# making connection pool to database
@login_required
def makeconnection(request):
    try:
        global postgres_pool
        if request.session['database_type'] == 'mysql':
            conn = mysql_pool.get_connection()
        elif request.session['database_type'] == 'postgres':
            conn = postgres_pool.getconn()
        return conn
    except Exception as ex:
        print('inside makeconnection final exception')
        print(ex)
        return None


# server select view

class DatabaseConfigView(LoginRequiredMixin, generic.FormView):
    form_class = ConnectServerForm
    template_name = 'connectserver.html'
    success_url = '/'

    def get_context_data(self, **kwargs):
        try:
            global mysql_pool
            global postgres_pool
            mysql_pool = ''
            postgres_pool = ''
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
            self.request.session['port'] = port
            self.request.session['host'] = host
            self.request.session['password'] = password
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
            print(ex)
            try:
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)
            except Exception as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error='Unknown Error')
        except mysql.connector.errors.Error as ex:
            print(ex)
            try:
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)
            except Exception as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error='Unknown Error')
        except Exception as ex:
            print(ex)
            print('final exception')
            try:
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)
            except Exception as ex:
                print(ex)
                return redirect('myapp:ConnectServer', error='Unknown Error')
        return redirect(reverse('myapp:ListDatabaseView'))


# rendering table list into selectdatabase.html from selecttable.html
@login_required
def createModel(request):
    if request.method == 'GET':
        tablelist = []
        database = request.GET.get('name')
        request.session['database'] = database
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
                global postgres_pool
                try:
                    postgres_pool.closeall()
                except Exception as ex:
                    print(ex)
                try:
                    postgres_pool = psycopg2.pool.ThreadedConnectionPool(1, 100, user=request.session['username'],
                                                                         password=request.session['password'],
                                                                         host=request.session['host'],
                                                                         port=request.session['port'],
                                                                         database=database)
                    conn = postgres_pool.getconn()
                except Exception as ex:
                    print(ex)
                conn = makeconnection(request)
                cursor = conn.cursor()
                showtables = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
                cursor.execute(showtables)
                for tables in cursor.fetchall():
                    print(tables[1])
                    tablelist.append(tables[1])
                cursor.close()
                conn.close()

            return render(request, 'selecttable.html', {'tables': tablelist})
        except Exception as ex:
            print(ex)
            return render(request, 'selecttable.html', {'tables': tablelist})


# database list, tablelist, csvfile GET and POST method
@login_required
@csrf_exempt
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
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)
            except mysql.connector.errors.Error as ex:
                print(ex)
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)
            except Exception as ex:
                print(str(ex))
                if str(ex) == "'NoneType' object has no attribute 'cursor'":
                    return redirect('myapp:ConnectServer', error='Not connected')
                return redirect('myapp:ConnectServer', error=ex)

            # executing show database query according to server
            if request.session['database_type'] == 'postgres':
                cursor.execute(
                    'SELECT datname FROM pg_database WHERE datistemplate = false;')
            elif request.session['database_type'] == 'mysql':
                cursor.execute('SHOW DATABASES')
            for database in cursor.fetchall():
                databases.append(database[0])
            cursor.close()
            conn.close()
            # rendering database list
            error = None
            return render(request, 'selectdatabase.html', {'databases': databases, 'error': error})

        if request.method == 'POST':
            try:
                print('method callled')
                global subprocess_thread_list
                csvfile = request.FILES.get('csvfile')
                table = request.POST.get('table')
                database = request.POST.get('database')
                username = request.session['username']
                database_type = request.session['database_type']
                port = request.session['port']
                host = request.session['host']
                table_list = request.POST.getlist('table_list[]')
                header_list = request.POST.getlist('header_list[]')

                # header and table_field validation starts
                if len(header_list)-header_list.count('None') <= 0:
                    raise FieldNotSet('please select at least one header')
                if len(table_list)-table_list.count('None') <= 0:
                    raise FieldNotSet('please select at least one table field')
                for header, field in zip(header_list, table_list):
                    if (header == 'None' and field != 'None') or (field == 'None' and header != 'None'):
                        raise(FieldNotSet(
                            'some header and table fields are not matching'))

                table_list = list(filter(lambda a: a != 'None', table_list))
                header_list = list(filter(lambda a: a != 'None', header_list))
                # header and table_field validation ends

                if csvfile == None:
                    raise Exception('No file is Uploaded!!')
                try:
                    csv_dataframe = pandas.read_csv(csvfile)[header_list]
                except Exception as ex:
                    print(ex)
                    raise FieldNotSet('columns are not present in table')
                if csvfile == None:
                    return JsonResponse({'error_rows': 'file not found or corrupt file!!'})

                # creating hash to check whether this request is already fulfiled or not.
                csvfile.seek(0, 0)
                data = csvfile.read()
                data = data.decode('utf-8')
                table_list_sorted = sorted(table_list)
                header_list_sorted = sorted(header_list)
                data += str(table_list_sorted) + str(header_list)
                data = bytes(data, 'utf-8')
                md5_hash = hashlib.md5(data)
                digest = md5_hash.hexdigest()

                error_model = models.CsvErrorFile.objects.filter(user=request.user, server_name=database_type,
                                                                 server_username=username, server_port=port, server_host=host,
                                                                 server_database=database, server_table=table, uploaded_file_hash=digest,
                                                                 commited=True).exclude(process_state='error')
                if len(error_model) != 0:
                    return JsonResponse({'error_rows': 'request is already fulfilled, check history for error data file'})
                else:
                    error_model = models.CsvErrorFile(user=request.user, server_name=database_type, server_username=username,
                                                      server_port=port, server_host=host, server_database=database, server_table=table,
                                                      uploaded_file=csvfile, process_state='processing',
                                                      uploaded_file_hash=digest, commited=True)
                    error_model.save()
                    model_pk = error_model.pk

                # converting csv data for database entry
                # seperating header from csv
                header, raw_header, data = csvSplitter(csv_dataframe)
                raw_header = tuple(table_list)
                header = str(raw_header)
                if len(tuple(table_list)) == 1:
                    headindex = header.rfind(',')
                    header = header[:headindex]+''+header[headindex+1:]
                    header = header.replace("'", "")
                else:
                    header = header.replace("'", "")

                # setting global counter for thread
                global is_thread_manager_running
                start_time = time.time()

                # creating csvchecksubprocess thread and append in master thread list
                commit = True
                t1 = threading.Thread(target=csvThreadCreator, args=(
                    request, database, data, table, header, raw_header, csv_dataframe, commit, model_pk))
                if parallel_user_thread_dict.get(request.user.username) == None:
                    parallel_user_thread_dict[request.user.username] = [t1]
                else:
                    parallel_user_thread_dict[request.user.username].insert(
                        0, t1)
                # if thread manager is shutdown then turn it on.
                if is_thread_manager_running == False:
                    is_thread_manager_running = True
                    print('calling thread manager from listDatabaseView for commiting')
                    # master thread creator for thread manager which maintains queue.
                    t2 = threading.Thread(target=threadManager)
                    t2.start()
                print('csv check thread creation and handle time for starting background processing:',
                      time.time()-start_time)
                return JsonResponse({'error_rows': 'Go to logs for more information'})
            except FieldNotSet as fns:
                print(str(fns))
                return JsonResponse({'error_rows': str(fns)})
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
    except Exception as ex:
        print(ex)
        logout(request)
        return redirect('myapp:ConnectServerRedirect')


# getting form using ajax try to put data inside database but not commiting.
@login_required
@csrf_exempt
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

            # header and table field list and validator
            table_list = request.POST.getlist('table_list[]')
            header_list = request.POST.getlist('header_list[]')

            # header and table_field validation starts
            if len(header_list)-header_list.count('None') <= 0:
                raise FieldNotSet('please select at least one header')
            if len(table_list)-table_list.count('None') <= 0:
                raise FieldNotSet('please select at least one table field')
            for header, field in zip(header_list, table_list):
                if (header == 'None' and field != 'None') or (field == 'None' and header != 'None'):
                    raise(FieldNotSet(
                        'some header and table fields are not matching'))

            table_list = list(filter(lambda a: a != 'None', table_list))
            header_list = list(filter(lambda a: a != 'None', header_list))
            # header and table_field validation ends

            if csvfile == None:
                raise Exception('No file is Uploaded!!')
            try:
                csv_dataframe = pandas.read_csv(csvfile)[header_list]
            except Exception as ex:
                print(ex)
                raise FieldNotSet('columns are not present in table')
            if csvfile == None:
                return JsonResponse({'error_rows': 'file not found or corrupt file!!'})

            # creating hash to check whether this request is already fulfiled or not.
            csvfile.seek(0, 0)
            data = csvfile.read()
            data = data.decode('utf-8')
            table_list_sorted = sorted(table_list)
            header_list_sorted = sorted(header_list)
            data += str(table_list_sorted) + str(header_list)
            data = bytes(data, 'utf-8')
            md5_hash = hashlib.md5(data)
            digest = md5_hash.hexdigest()

            # making database entry for this request.
            error_model = models.CsvErrorFile.objects.filter(user=request.user, server_name=database_type, server_username=username,
                                                             server_port=port, server_host=host, server_database=database,
                                                             server_table=table, uploaded_file_hash=digest, commited=False).exclude(process_state='error')
            # if request is already fulfilled then give message.
            if len(error_model) != 0:
                return JsonResponse({'error_rows': 'request is already fulfilled, check history for error data file'})
            # else create new request
            else:
                error_model = models.CsvErrorFile(user=request.user, server_name=database_type,
                                                  server_username=username, server_port=port,
                                                  server_host=host, server_database=database, server_table=table,
                                                  uploaded_file=csvfile, process_state='processing',
                                                  uploaded_file_hash=digest, commited=False)
                error_model.save()
                model_pk = error_model.pk

            # converting csv data for database entry
            # seperating header from csv
            header, raw_header, data = csvSplitter(csv_dataframe)
            raw_header = tuple(table_list)
            header = str(raw_header)
            if len(tuple(table_list)) == 1:
                headindex = header.rfind(',')
                header = header[:headindex]+''+header[headindex+1:]
                header = header.replace("'", "")
            else:
                header = header.replace("'", "")

            # setting global counter for thread
            # global total_counter
            global is_thread_manager_running
            global parallel_user_thread_dict
            start_time = time.time()

            # creating csvchecksubprocess thread and append in master thread list
            commit = False
            t1 = threading.Thread(target=csvThreadCreator, args=(
                request, database, data, table, header, raw_header, csv_dataframe, commit, model_pk))
            if parallel_user_thread_dict.get(request.user.username) == None:
                parallel_user_thread_dict[request.user.username] = [t1]
            else:
                parallel_user_thread_dict[request.user.username].append(t1)
            if is_thread_manager_running == False:
                is_thread_manager_running = True
                print('calling thread manager from csvcheck')
                # master thread creator for thread manager which maintains queue.
                t2 = threading.Thread(target=threadManager)
                t2.start()
            print('csv check thread creation and handle time for starting background processing:',
                  time.time()-start_time)
            return JsonResponse({'error_rows': 'Go to logs for more information'})

        except FieldNotSet as fns:
            print(str(fns))
            return JsonResponse({'error_rows': str(fns)})
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
        global is_thread_manager_running
        print('thread manager is running')
        start_time = time.time()

        start_time = time.time()
        global parallel_user_thread_dict
        while not all(x == [] for x in parallel_user_thread_dict.values()):
            for k, v in parallel_user_thread_dict.items():
                if len(v) > 0:
                    if v[0].is_alive():
                        time.sleep(0.001)
                    elif v[0]._is_stopped:
                        v.pop(0)
                        parallel_user_thread_dict[k] = v

                    elif v[0]._initialized:
                        v[0].start()
            time.sleep(1)
        is_thread_manager_running = False
        print('all task completes, thread manager is going to sleep=============')
    except Exception as ex:
        print('exception in thread manager -----------------------')
        print(ex)

# create multitheaded check csv subprocess, called by threadManager, aslo create error file


def csvThreadCreator(request, database, data, table, header, raw_header, user, commit, model_pk):
    try:
        error_rows = []
        thread_count = 5
        start_time = time.time()

        subprocess_thread_list = []
        # creating multiple threads for checking csv
        if request.session['database_type'] == 'mysql':
            check_columns = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`= '%s' AND `TABLE_NAME`= '%s';" % (
                database, table)
        elif request.session['database_type'] == 'postgres':
            check_columns = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';" % (
                table)
        conn = makeconnection(request)
        cursor = conn.cursor()
        cursor.execute(check_columns)
        column_list = [v for v in cursor.fetchall()]
        flag = 0
        cursor.close()
        conn.close()
        if all(x in [ls[0] for ls in column_list] for x in raw_header):
            print('ok')
        else:
            raise Exception('CSV header is not matching with database table')

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

        # creating file if error list contains data.
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
            error_model = models.CsvErrorFile.objects.get(pk=model_pk)
            error_model.error_file = File(csvfile)
            error_model.process_state = 'completed'
            error_model.message = 'Errro File Generated'
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
            error_model.message = ex
            error_model.save()
        except Exception as ex:
            print('error while updating model state to error inside csvThreadCreator')
            print(ex)


# its a thread process of csvthread, called by csvThreadCreator
def csvThread(request, database, data, table, header, startvalue, jumpvalue, commit):
    error_rows = []
    try:
        # creating connection to database
        database_type = request.session['database_type']
        conn = makeconnection(request)
        cursor = conn.cursor()
        if database_type == 'mysql':
            databasequery = 'USE %s' % (database)
            cursor.execute(databasequery)

        # checking if error occur while try to adding data into table, if error occured add into list
        for row_count, row in enumerate(data):
            try:
                insertquery = "INSERT INTO %s %s VALUES %s;" % (
                    table, header, row)
                cursor.execute(insertquery)
                time.sleep(0.0005)
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
            print("error in csvThread, can't close cursor or conn")
            print(ex)
        finally:
            print(len(error_rows), '-------finally')
            return error_rows
# thread management ends here

# it seperates header, data and raw_header from csv file data.


def csvSplitter(user):
    try:
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
        return header, raw_header, data
    except Exception as ex:
        print(ex)
        return None, None, None


@csrf_exempt
def columnMatcher(request):
    if request.method == 'POST':
        try:
            csvfile = request.FILES.get('csvfile')
            table = request.POST.get('table')
            database = request.POST.get('database')
            if csvfile == None:
                print('null')
                raise Exception('please fill all the fields')
            print(csvfile, table, database)
            csvfile_data = pandas.read_csv(csvfile)
            header, raw_header, data = csvSplitter(csvfile_data)
            if request.session['database_type'] == 'mysql':
                column_list_of_table_query = """SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
                                            WHERE TABLE_NAME = '%s' and table_schema='%s'""" % (table, database)
            elif request.session['database_type'] == 'postgres':
                global postgres_pool
                try:
                    postgres_pool.closeall()
                except Exception as ex:
                    print(ex)
                try:
                    postgres_pool = psycopg2.pool.ThreadedConnectionPool(1, 100, user=request.session['username'],
                                                                         password=request.session['password'],
                                                                         host=request.session['host'],
                                                                         port=request.session['port'],
                                                                         database=database)
                    conn = postgres_pool.getconn()
                except Exception as ex:
                    print(ex)
                column_list_of_table_query = """SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
                                            WHERE TABLE_NAME = '%s'""" % (table)
            table_columns_list = []
            conn = makeconnection(request)
            cursor = conn.cursor()
            cursor.execute(column_list_of_table_query)
            print('ok')
            for column in cursor.fetchall():
                print(column)
                column = column[0].strip()
                table_columns_list.append(column)
            raw_header = list(raw_header)
            raw_header = [header.strip() for header in raw_header]
            # print('--==--==',raw_header, table_columns_list)
            cursor.close()
            conn.close()
            return render(request, 'column_matcher.html', {'raw_header': raw_header, 'table_columns_list': table_columns_list})
        except Exception as ex:
            print(ex)
            return JsonResponse({'error_rows': str(ex)})


class FieldNotSet(Exception):
    def __init__(self, message, payload=None):
        self.message = message
        self.payload = payload  # you could add more args

    def __str__(self):
        return str(self.message)


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
