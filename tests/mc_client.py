#!/usr/bin/env python
import socket

host = 'localhost'
port = 15721
size = 1024

query = 'GET key\r\n'

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host,port))
s.send(query*100)
# data = s.recv(size)
s.close()
# print 'Received:', data 