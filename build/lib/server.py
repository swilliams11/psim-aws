#!/usr/bin/env python

import socket, threading              

"""
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


"""

def run_t():
    print "\nstart run_t()"
    t = threading.Thread(target=run, \
            args=())
    t.setDaemon(True)
    t.start()    
#
# start the server and listen on static port.
#
def run():
    threadName = threading.currentThread().getName()
    print "\n%s -> start run()" % (threadName)
    try:
        s = socket.socket()  
    except socket.error as msg:
        print "\n%s -> Node %s -> could not create socket. %s" % (threadName, self.rank, msg)
        sys.exit(1)
    try:
        s.bind(('', Server.port))        
        print "\n%s -> Node %s -> socket bound to %s" % (threadName, self.rank, Server.port)
        s.listen(5)      
        print "\n%s -> Node %s -> socket listening..." % (threadName, self.rank)

    except socket.error as msg:
        print "\n%s -> Node %s -> error during binding or listening. -> %s" % (threadName, self.rank, msg)
        s.close()
        s = None
        sys.exit(1)
    
    while True:
        conn, addr = s.accept()     
        print '\n%s -> Node %s -> connected to %s' % (threadName, self.rank, self.addr)
        data = conn.receive(1024)
        #receive(data)

#
#Threaded version
# start the server and listen on static port.
#
def run_threaded():
    s.bind(('', port))        
    print "\n%s -> socket bound to %s" % (self.rank, port)
    s.listen(5)      
    print "\n%s -> socket listening..." % (self.rank)
    while True:
        conn, addr = s.accept()     
        print 'Connected from %s' % (addr)
        t = threading.Thread(target=handle_connection, \
            args=(self, conn, addr))
        t.start()




print "starting the server..."
run_t()
for i in range(10000):
	print i

