#!/usr/bin/env python

import socket               

s = socket.socket()         
port = 9876                
# connect to the server on local computer
s.connect(('127.0.0.1', port))
print s.recv(1024)
s.close()  
