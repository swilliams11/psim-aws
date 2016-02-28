
from psim import *

d = 2
#d = 1
p = int(2**d-1)

#p = 2

comm = PSim(p, topology = SWITCH)
comm.log( '\ninitialized PSim\n')

#random.seed(comm.rank)
x = comm.rank**2

if comm.rank<int(2**(d-1)-1):
    left = 2*comm.rank+1
    right = 2*comm.rank+2
    a = comm.recv(left)
    b = comm.recv(right)
    x = x + a + b

if comm.rank>0:
    parent = int((comm.rank-1)/2)
    comm.send(parent,x)
else:
    comm.log('\nx = %s' % (str(x)))