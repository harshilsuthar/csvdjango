# import socket
# mysocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)        
# host = "127.0.0.1"
# port = 9100
# try:          
#     mysocket.connect((host, port)) #connecting to host
#     print('\n====czsdd')
#     mysocket.send(b"^XA^A0N,50,50^FO50,50^FDSocket Test^FS^XZ")#using bytes
#     mysocket.close () #closing connection
# except Exception as ex:
#     print(ex)
#     print("Error with the connection")