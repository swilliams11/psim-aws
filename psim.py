# Created by Massimo Di Pierro - BSD License

import os, string, cPickle, time, math, itertools
import socket
import shlex
import threading
import sys

def BUS(i,j):
    return True

def SWITCH(i,j):
    return True

def MESH1(p):
    return lambda i,j,p=p: (i-j)**2==1

def TORUS1(p):
    return lambda i,j,p=p: (i-j+p)%p==1 or (j-i+p)%p==1

def MESH2(p):
    q=int(math.sqrt(p)+0.1)
    return lambda i,j,q=q: ((i%q-j%q)**2,(i/q-j/q)**2) in [(1,0),(0,1)]

def TORUS2(p):
    q=int(math.sqrt(p)+0.1)
    return lambda i,j,q=q: ((i%q-j%q+q)%q,(i/q-j/q+q)%q) in [(0,1),(1,0)] or \
                           ((j%q-i%q+q)%q,(j/q-i/q+q)%q) in [(0,1),(1,0)]
def TREE(i,j):
    return i==int((j-1)/2) or j==int((i-1)/2)

#
#Server object 
#
class Server(object):
    port = 9876

    def log(self,message):
        """
        logs the message into self._logfile
        """
        if self.logfile!=None:
            self.logfile.write(message)

    #
    #constructor
    #
    def __init__(self, rank, plocal, pipes, logfilename='server.log'):
        """
        """
        self.privateIpNodeDictionary = {}
        self.nodeToPrivateIpDictionary = {}
        #self.dataArrivedEvent = threading.Event()
        self.p = plocal
        self.data = None
        self.conn = None
        self.addr = None
        self.pipes = pipes
        #logfilename = 'server' + str(rank) + '.log'
        logfilename = 'server.log'
        self.logfile = open(logfilename,'w', 0)

        #read the node and private IP address into a flow variable
        self.log("\nReading node list...")
        with open('nodelist', 'r') as f:
            for line in f:
                splitLine = shlex.split(line)
                index = int(splitLine[0]) #read node number
                self.privateIpNodeDictionary[index] = splitLine[1] #private IP address
                #self.privateIpNodeDictionary[index] = '127.0.0.1'
                self.log("\n node: %s ; private ip : %s" % (index, splitLine[1]))

        #create the node to private Ip dictionary
        for i in range(len(self.privateIpNodeDictionary)):
            privateIp = self.privateIpNodeDictionary[i]
            self.nodeToPrivateIpDictionary[privateIp] = i 

        self.log("\nstarting the server in new thread.")
        self.s = None
        self.rank = rank
        self.addr = self.privateIpNodeDictionary[rank]
        print("rank %s" % (rank))
        self.run_t()
        self.log("\nSTART: done.\n")

    def run_t(self):
        self.log("\nstart run_t()")
        t = threading.Thread(target=self.run, \
                args=())
        #t.setDaemon(True)
        t.start()    


    #
    # start the server and listen on static port.
    #
    def run(self):
        threadName = threading.currentThread().getName()
        self.log("\n%s -> start run()" % (threadName))
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        except socket.error as msg:
            self.log("\n%s -> Node %s -> could not create socket. %s" % (threadName, self.rank, msg))
            sys.exit(1)
        
        try:
            self.log("\n%s -> Node %s -> going to bind to socket %s" % (threadName, self.rank, Server.port))
            self.s.bind(('', Server.port))        
            self.log("\n%s -> Node %s -> socket bound to %s" % (threadName, self.rank, Server.port))
            self.s.listen(50)      
            self.log("\n%s -> Node %s -> socket listening..." % (threadName, self.rank))

        except socket.error as msg:
            self.log("\n%s -> Node %s -> error during binding or listening. -> %s" % (threadName, self.rank, msg))
            self.s.close()
            self.s = None
            sys.exit(1)
        
        while True:
            self.receive2()
           

    #
    #receive the data
    #
    def receive2(self):
        self.log('\nNode %s -> in server.receive()' % (self.rank))
        threadName = threading.currentThread().getName()
        self.log('\n%s -> Node %s -> waiting for connection @ %s' % (threadName, self.rank, self.addr))
        #self.s = socket.socket()    
        #self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #returns a connect and an address (tuple)
        self.conn, addr = self.s.accept()     
        self.log('\n%s -> Node %s -> connected to %s' % (threadName, self.rank, addr))
        data = self.conn.recv(1024)
        data = cPickle.loads(data) #unpack the data
        #lookup the node to send to
        j = self.nodeToPrivateIpDictionary[addr[0]]  #use IP address of the tuple
        self.log('\n%s -> Node %s -> Writing data from %s to file... Data=%s\n' % (threadName, self.rank, addr, data))
        self.write_to_file(j, data)

        #receive(data)
        #datatemp = self.data
        #self.data = None
        #Do not return data from receive, it should read from a pipe instead
        #return self.data


    #
    #write the data to file
    #copied from PSim._send()
    #
    def write_to_file(self, j, data):
        """
        sends data to process #j
        """
        if j<0 or j>=self.p: #self.nprocs:
            self.log("\nprocess %i: write_to_file(sendTo=%i,...) failed!\n" % (self.rank,j))
            raise Exception
        self.log("\nprocess %i: write_to_file(sendTo=%i,data=%s) starting...\n" % \
                 (self.rank,j,repr(data)))
        s = cPickle.dumps(data) #pack the data
        #s = data
        try:
            self.log("process %i: self.pipes[%s,%s][1] = %s\n" %(self.rank, self.rank, j, self.pipes[self.rank,j][1]))
            os.write(self.pipes[self.rank,j][1], string.zfill(str(len(s)),20))
            os.write(self.pipes[self.rank,j][1], s)
            self.log("process %i: write_to_file(sendTo=%i,data=%s) success.\n" % \
                 (self.rank,j,repr(data)))
            #self.log("process %i: setting the dataArrivedEvent.set()\n" % (self.rank))
            #self.dataArrivedEvent.set() #sets the flag so the other thread can continue
        except Exception, e:
            self.log("process %i: ERROR writing to pipe!!!\n" % (self.rank))
            raise e


    #
    #Handle the connection
    #
    def handle_connection (self, conn, addr):
        threadName = threading.currentThread().getName()
        self.log('%s -> starting handling connection from %s' % (threadName, addr))



class PSim(object):
    counter = itertools.count()

    def log(self,message):
        """
        logs the message into self._logfile
        """
        if self.logfile!=None:
            self.logfile.write(message)

    def __init__(self,p,topology=SWITCH,logfilename='psim.log'):
        """
        forks p-1 processes and creates p*p
        """
        self.s = None
        self.logfile = open('psim.log', 'w', 0)
        self.topology = topology
        self.log("START: creating %i parallel processes\n" % p)
        self.nprocs = p
        self.privateIpNodeDictionary = {}
        #startup the amazon EC2 instances here
        f = open('rank', 'r')
        self.rank = int(f.read())
        self.log("I am node %s\n" % (self.rank))
        
        #creates a dictionary
        self.pipes = {}
        for i in range(p):
            for j in range(p):
                #this creates an os pipe for (0,0), (0,1), (0,2)
                self.pipes[i,j] = os.pipe()

        #Start the server
        self.server = Server(rank=self.rank, plocal=p, pipes=self.pipes)


        #read the node and private IP address into a variable
        with open('nodelist', 'r') as f:
            for line in f:
                splitLine = shlex.split(line)
                #read node number and set that as index assign IP address to node number
                self.privateIpNodeDictionary[int(splitLine[0])] = splitLine[1]
                #self.privateIpNodeDictionary[int(splitLine[0])] = '127.0.0.1'

        #f.close()
        self.log("START: done.\n")


    def send(self,j,data):
        if not self.topology(self.rank,j):
            raise RuntimeError, 'topology violation'
        self._send(j,data)


    def _send(self,j,data):
        """
        sends data to process #j
        """
        if j<0 or j>=self.nprocs:
            self.log("process %i: send(%i,...) failed!\n" % (self.rank,j))
            raise Exception
        self.log("process %i: send(%i,%s) starting...\n" % \
                 (self.rank,j,repr(data)))
        s = cPickle.dumps(data)
        #s = data
        #Write to the TCP socket here with the private IP address
        self.server_send(j, s)
        #os.write(self.pipes[self.rank,j][1], string.zfill(str(len(s)),10))
        #os.write(self.pipes[self.rank,j][1], s)
        self.log("process %i: send(%i,%s) success.\n" % \
                 (self.rank,j,repr(data)))

    
    #
    # send the data
    # open the connection and send the data
    #
    def server_send(self, node, data):
        ip = self.privateIpNodeDictionary[self.rank]
        self.log("\nsending data from %s %s to %s" % (self.rank, ip, node))
        sendSuccessful = False
        while not sendSuccessful:
            try: 
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
                self.log('\nnode %s %s Connecting to %s %s on port %s' % (self.rank, ip, node, self.privateIpNodeDictionary[node], Server.port))
                self.s.connect((self.privateIpNodeDictionary[node], Server.port))
                self.s.sendall(data)
                self.log('\ndata sent successfully!\n')
                self.s.close()  
                sendSuccessful = True
            except socket.error as msg:
                sendSuccessful = False
                self.log("\nSleeping... send was not successful!")
                time.sleep(.5)


    def recv(self,j):
        if not self.topology(self.rank,j):
            raise RuntimeError, 'topology violation'
        return self._recv(j)


    def _recv(self,j):
        """
        returns the data recvd from process #j
        """
        if j<0 or j>=self.nprocs:
            self.log("process %i: recv(%i) failed!\n" % (self.rank,j))
            raise RuntimeError
        self.log("process %i: recv(%i) starting...\n" % (self.rank,j))
        #attempt to read the data from the socket here.
        #self.log("process %i: dataArrivedEvent.wait() called.\n" % (self.rank))
        #self.server.dataArrivedEvent.wait()
        s = None
        try:
            #self.log("process %i is about to read size...\n" % (self.rank))
            self.log("process %i pipes (%s, %s) = %s\n" % (self.rank, self.rank,j, self.pipes[self.rank,j][0]))
            #self.log("process %i pipes (%s, %s) = %s\n" % (self.rank, j, self.rank, self.pipes[j,self.rank][0]))
            size=int(os.read(self.pipes[self.rank,j][0], 20))
            self.log('size = %d\n' % (size))
            s=os.read(self.pipes[self.rank,j][0],size)
            self.log('cPickle data = %s\n' % (s))
        except Exception, e:
            self.log("process %i: COMMUNICATION ERROR!!!\n" % (self.rank))
            raise e

        data=cPickle.loads(s)
        self.log('data = %s\n' % (data))
        self.log("process %i: recv(%i) done.\n" % (self.rank,j))
        #self.log("process %i: dataArrivedEvent.clear() called.\n" % (self.rank))
        #self.server.dataArrivedEvent.clear()
        return data

   

    def one2all_broadcast(self, source, value=None):
        self.log("process %i: BEGIN one2all_broadcast(%i,%s)\n" % \
                 (self.rank,source, repr(value)))
        if self.rank==source:
            for i in range(0, self.nprocs):
                if i!=source:
                    self._send(i,value)
        else:
            value=self._recv(source)
        self.log("process %i: END one2all_broadcast(%i,%s)\n" % \
                 (self.rank,source, repr(value)))
        return value

    def all2all_broadcast(self, value):
        self.log("process %i: BEGIN all2all_broadcast(%s)\n" % \
                 (self.rank, repr(value)))
        vector=self.all2one_collect(0,value)
        vector=self.one2all_broadcast(0,vector)
        self.log("process %i: END all2all_broadcast(%s)\n" % \
                 (self.rank, repr(value)))
        return vector

    def one2all_scatter(self,source,data):
        self.log('process %i: BEGIN all2one_scatter(%i,%s)\n' % \
                 (self.rank,source,repr(data)))
        if self.rank==source:
             h, reminder = divmod(len(data),self.nprocs)
             if reminder: h+=1
             for i in range(self.nprocs):
                 self._send(i,data[i*h:i*h+h])
        vector = self._recv(source)
        self.log('process %i: END all2one_scatter(%i,%s)\n' % \
                 (self.rank,source,repr(data)))
        return vector

    def all2one_collect(self,destination,data):
        self.log("process %i: BEGIN all2one_collect(%i,%s)\n" % \
                 (self.rank,destination,repr(data)))
        self._send(destination,data)
        if self.rank==destination:
            vector = [self._recv(i) for i in range(self.nprocs)]
        else:
            vector = []
        self.log("process %i: END all2one_collect(%i,%s)\n" % \
                 (self.rank,destination,repr(data)))
        return vector

    def all2one_reduce(self,destination,value,op=lambda a,b:a+b):
        self.log("process %i: BEGIN all2one_reduce(%s)\n" % \
                 (self.rank,repr(value)))
        self._send(destination,value)
        if self.rank==destination:
            result = reduce(op,[self._recv(i) for i in range(self.nprocs)])
        else:
            result = None
        self.log("process %i: END all2one_reduce(%s)\n" % \
                 (self.rank,repr(value)))
        return result

    def all2all_reduce(self,value,op=lambda a,b:a+b):
        self.log("process %i: BEGIN all2all_reduce(%s)\n" % \
                 (self.rank,repr(value)))
        result=self.all2one_reduce(0,value,op)
        result=self.one2all_broadcast(0,result)
        self.log("process %i: END all2all_reduce(%s)\n" % \
                 (self.rank,repr(value)))
        return result

    @staticmethod
    def sum(x,y): return x+y
    @staticmethod
    def mul(x,y): return x*y
    @staticmethod
    def max(x,y): return max(x,y)
    @staticmethod
    def min(x,y): return min(x,y)

    def barrier(self):
        self.log("process %i: BEGIN barrier()\n" % (self.rank))
        self.all2all_broadcast(0)
        self.log("process %i: END barrier()\n" % (self.rank))
        return

def test():
    comm=PSim(5,SWITCH)
    if comm.rank==0: print 'start test'
    a=sum(comm.all2all_broadcast(comm.rank))
    comm.barrier()
    b=comm.all2all_reduce(comm.rank)
    if a!=10 or a!=b:
        print 'from process', comm.rank
        raise Exception
    if comm.rank==0: print 'test passed'

if __name__=='__main__': test()

def scalar_product_test1(n,p):
    import random
    from psim import PSim
    comm = PSim(p)
    h = n/p
    if comm.rank==0:
        a = [random.random() for i in range(n)]
        b = [random.random() for i in range(n)]
        for k in range(1,p):
            comm.send(k, a[k*h:k*h+h])
            comm.send(k, b[k*h:k*h+h])
    else:
        a = comm.recv(0)
        b = comm.recv(0)
    scalar = sum(a[i]*b[i] for i in range(h))
    if comm.rank == 0:
        for k in range(1,p):
            scalar += comm.recv(k)
        print scalar
    else:
        comm.send(0,scalar)

def scalar_product_test2(n,p):
    import random
    from psim import PSim
    comm = PSim(p)
    a = b = None
    if comm.rank==0:
        a = [random.random() for i in range(n)]
        b = [random.random() for i in range(n)]
    a = comm.one2all_scatter(0,a)
    b = comm.one2all_scatter(0,b)

    scalar = sum(a[i]*b[i] for i in range(len(a)))

    scalar = comm.all2one_reduce(0,scalar)
    if comm.rank == 0:
        print scalar


def mergesort(A, p=0, r=None):
    if r is None: r = len(A)
    if p<r-1:
        q = int((p+r)/2)
        mergesort(A,p,q)
        mergesort(A,q,r)
        merge(A,p,q,r)

def merge(A,p,q,r):
    B,i,j = [],p,q
    while True:
        if A[i]<=A[j]:
            B.append(A[i])
            i=i+1
        else:
            B.append(A[j])
            j=j+1
        if i==q:
            while j<r:
                B.append(A[j])
                j=j+1
            break
        if j==r:
            while i<q:
                B.append(A[i])
                i=i+1
            break
    A[p:r]=B

def mergesort_test(n,p):
    import random
    from psim import PSim
    comm = PSim(p)
    if comm.rank==0:
        data = [random.random() for i in range(n)]
        comm.send(1, data[n/2:])
        mergesort(data,0,n/2)
        data[n/2:] = comm.recv(1)
        merge(data,0,n/2,n)
        print data
    else:
        data = comm.recv(0)
        mergesort(data)
        comm.send(0,data)

if __name__=='__main__':
    import os,doctest
    if not os.path.exists('images'): os.mkdir('images')
    doctest.testmod(optionflags=doctest.ELLIPSIS)