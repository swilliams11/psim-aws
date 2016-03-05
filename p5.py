
from psim import *

d = 3
#d = 1
p = int(2**d-1)

#p = 2

comm = PSim(p, topology = SWITCH)
logfile = open('py.log','w', 0)
comm.log( '\ninitialized PSim\n')

#random.seed(comm.rank)
x = comm.rank**2

if comm.rank<int(2**(d-1)-1):
    left = 2*comm.rank+1
    right = 2*comm.rank+2
    a = comm.recv(left)
    logfile.write('LEFT - rcvd a = %s\n' % (a))
    b = comm.recv(right)
    logfile.write('RIGHT - rcvd b = %s\n' % (b))
    x = x + a + b
    logfile.write('rank %s x = %s\n' % (comm.rank, x))

if comm.rank>0:
	parent = int((comm.rank-1)/2)
	logfile.write('Node %s is sending to parent %s; x = %s\n' % (comm.rank, parent, x))
	comm.send(parent,x)
else:
	logfile.write('I am node %s and I calculated %s\n' % (comm.rank, x))
	logfile.write('\nx = %s\n' % (str(x)))
	print('x = %s' % (str(x)))

logfile.close()