import mysql
import mysql.connector.pooling
import pandas

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


mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="mypool", pool_size=32, user='root', passwd='root', host='localhost', port=3306)

conn = mysql_pool.get_connection()
cursor = conn.cursor()
foreing_key_get_query = """select referenced_table_name,referenced_column_name,column_name from information_schema.key_column_usage where table_name ='%s' and constraint_schema='%s';"""%('rental_vehicletype','rental')
print(foreing_key_get_query)
user = pandas.read_csv('D://django test/csvdjango/media/uploads/small_csv.csv')
header, raw_header, data = csvSplitter(user)
cursor.execute(foreing_key_get_query)
raw_header = list(raw_header)
for data1 in cursor.fetchall():
    
    
    if data1[0] != None:
        print(data[0])
        idx = raw_header.index(data1[2])
        st = list({row[idx] for row in user.values})
        print(st)
        print(idx)
        print(data1)

cursor.close()
conn.close()
mysql_pool._remove_connections()
