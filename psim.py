# Created by Massimo Di Pierro - BSD License

import os, string, cPickle, time, math
import socket
import shlex
import threading

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
    def __init__(self, rank, logfilename='server.log'):
        """
        """
        self.rank = rank
        print("rank %s" % (rank))
        self.logfile = open(logfilename,'w', 0)
        self.logfile.write("TEST 1")
        print("Initializing server\n")
        self.log("Initializing server\n")
        self.privateIpNodeDictionary = {}
        self.data = None
        self.conn = None
        self.addr = None

        #read the node and private IP address into a flow variable
        with open('nodelist', 'r') as f:
            for line in f:
                splitLine = shlex.split(line)
                index = int(splitLine[1]) #read node number
                self.privateIpNodeDictionary[index] = splitLine[0] #private IP address
                print("private ip : %s" % (splitLine[0]))
        
        self.run_t()
        #self.run_threaded2()
        print("START: done.\n")
        self.log("START: done.\n")


    


    #
    # send the data
    # open the connection and send the data
    #
    def send(self, node, data):
        self.log("Server sending data from %s to %s" % (self.rank, node))
        s = socket.socket() 
        s.connect((self.privateIpNodeDictionary[node], port))
        s.sendall(data)
        #print s.recv(1024)
        s.close()


    #
    #receive the data
    #
    def receive(self):
        datatemp = self.data
        self.data = None
        return datatemp


    def run_t(self):
        t = threading.Thread(target=self.run, \
                args=(self))
        t.start()    
    #
    # start the server and listen on static port.
    #
    def run(self):
        s = socket.socket()  
        s.bind(('', Server.port))        
        print("Node %s -> socket bound to %s" % (self.rank, Server.port))
        self.log("Node %s -> socket bound to %s" % (self.rank, Server.port))
        s.listen(5)      
        print("Node %s -> socket listening..." % (self.rank))
        self.log("Node %s -> socket listening..." % (self.rank))
        while True:
            self.conn, self.addr = s.accept()     
            print('Node %s -> connected to %s' % (self.rank, self.addr))
            self.log('Node %s -> connected to %s' % (self.rank, self.addr))
            self.data = self.conn.receive(1024)
            #receive(data)

    #
    #Threaded version
    # start the server and listen on static port.
    #
    def run_threaded(self):
        s.bind(('', port))        
        self.log("%s -> socket bound to %s" % (self.rank, port))
        s.listen(5)      
        self.log("%s -> socket listening..." % (self.rank))
        while True:
            conn, addr = s.accept()     
            self.log('Connected from %s' % (addr))
            t = threading.Thread(target=handle_connection, \
                args=(self, conn, addr))
            t.start()


    #
    #Handle the connection
    #
    def handle_connection (self, conn, addr):
        threadName = threading.currentThread().getName()
        self.log('%s -> starting handling connection from %s' % (threadName, addr))




class PSim(object):
    
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
        self.logfile = logfilename and open(logfilename,'w', 0)
        self.topology = topology
        self.log("START: creating %i parallel processes\n" % p)
        self.nprocs = p
        self.privateIpNodeDictionary = {}
        #startup the amazon EC2 instances here
        f = open('rank', 'r')
        self.rank = int(f.read())
        self.server = Server(rank=self.rank)
        f.close()

        #read the node and private IP address into a flow variable
        with open('nodelist', 'r') as f:
            for line in f:
                splitLine = shlex.split(line)
                self.privateIpNodeDictionary[int(splitLine[1])] = splitLine[0]

        #Open start the server listening here
        self.log("START: done.\n")

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

        #Write to the TCP socket here with the private IP address
        self.server.send(s)
        #os.write(self.pipes[self.rank,j][1], string.zfill(str(len(s)),10))
        #os.write(self.pipes[self.rank,j][1], s)

        self.log("process %i: send(%i,%s) success.\n" % \
                 (self.rank,j,repr(data)))

    def send(self,j,data):
        if not self.topology(self.rank,j):
            raise RuntimeError, 'topology violation'
        self._send(j,data)

    def _recv(self,j):
        """
        returns the data recvd from process #j
        """
        if j<0 or j>=self.nprocs:
            self.log("process %i: recv(%i) failed!\n" % (self.rank,j))
            raise RuntimeError
        self.log("process %i: recv(%i) starting...\n" % (self.rank,j))
        #attempt to read the data from the socket here.
        try:
            s = self.server.receive()
            #size=int(os.read(self.pipes[j,self.rank][0],10))
            #s=os.read(self.pipes[j,self.rank][0],size)
        except Exception, e:
            self.log("process %i: COMMUNICATION ERROR!!!\n" % (self.rank))
            raise e
        data=cPickle.loads(s)
        self.log("process %i: recv(%i) done.\n" % (self.rank,j))
        return data

    def recv(self,j):
        if not self.topology(self.rank,j):
            raise RuntimeError, 'topology violation'
        return self._recv(j)

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