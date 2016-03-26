#!/usr/bin/env python
import socket

host = 'localhost'
port = 15721
size = 1024

query = 'GET key lkflklgnwrlg legwlrglwrgwlrgnklwrgnjl3rgk jfegheqglqejghrqgfh\r\n'

print query

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host,port))
s.send(query*10)
# data = s.recv(size)
for i in range(10):
	print 'Read (' +str(i+1) + '): ' + s.recv(1000)
s.close()
# print 'Received:', data 