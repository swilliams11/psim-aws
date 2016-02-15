#!/usr/bin/env python

import socket               

s = socket.socket()         
print "Created Socket"

# reserve a port on your computer in our
# case it is 12345 but it can be anything
port = 9876                

s.bind(('', port))        
print "socket bound to %s" %(port)

s.listen(5)     
print "socket listening..."            

while True:
   c, addr = s.accept()     
   print 'Connected from ', addr

   # send a thank you message to the client. 
   c.send('message received')
   # Close the connection with the client
   c.close() 
