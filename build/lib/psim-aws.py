#!/usr/bin/env python

import boto3.ec2
import os
import threading
import shlex
import getopt
import sys

instances = None
publicIps = None

#use the default configuration in .aws/config and .aws/credentials files
ec2 = boto3.resource('ec2')
ec2client = boto3.client('ec2')
waiter = ec2client.get_waiter('instance_status_ok')
#these should be removed
SecurityGroup = 'psim_security_group'
#KeyPairFile = 'amazonEC2pair.pem'
KeyPairFile = None
KeyPairName = None

PrivateIpNodeDictionary = {} #dictionary of private IPs

#ec2.create_instances(ImageId='ami-d93622b8', 
#    InstanceType='t1.micro', MinCount=1, MaxCount=1, DryRun=True)


def printUsage():
  print 'Usage: psim-aws -f [name] -h'
  print '' 
  print '-f Amazon private key (.pem) located in ~/.ssh/key.pem'
  print '-h Print this message'
  print ''
  print 'Typically, the "default" virtual host listens on HTTP.'
  print 'For an HTTPS-only app, use "-x secure".'


#
#Create a default security group
#
def create_security_group ():
	ec2.create_security_group(GroupName='psim_security_group', Description='psim security group');


#
#Removes default security group settings
#
def configure_security_group(sg):
	sg.revoke_ingress(IpPermissions=[
        {
            'IpProtocol': '-1',
            'FromPort': 0,
            'ToPort': 65535,
            'IpRanges': [
                {
                    'CidrIp': '0.0.0.0/0'
                }
            ]}])

	sg.authorize_ingress(IpPermissions=[
        {
            'IpProtocol': 'tcp',
            'FromPort': 22,
            'ToPort': 22,
            'IpRanges': [
                {
                    'CidrIp': '0.0.0.0/0'
                }
            ]
        },
        {
            'IpProtocol': 'tcp',
            'FromPort': 9876,
            'ToPort': 9876,
            'IpRanges': [
                {
                    'CidrIp': '0.0.0.0/0'
                }
            ]
        }
    	]
    )


#
#Create instances with the supplied KeyName and SecurityGroupName
#
def create_instances_with_params(SecurityGroupName, KeyNameString, p=1):
	return ec2.create_instances(ImageId='ami-d93622b8', \
        InstanceType='t1.micro', MinCount=1, MaxCount=p, \
		DryRun=False, \
		KeyName=KeyNameString, \
    	SecurityGroups=[
        	SecurityGroupName,
    	])


#
#Write the node list to a file
#node number and private ip address
#Add the public ip and node number to the nodeListByPublicIp dictionary
#
def write_node_list(insts):
    if(os.path.isfile('nodelist')):
        os.remove('nodelist')

	f = open('nodelist','w')
	for i in range(len(insts)):
		f.write(str(i))
		f.write(' ')
		f.write(insts[i].private_ip_address)
		f.write('\n')
        write_rank_file(insts[i].public_ip_address, i)
	
	f.close()

#
#write the rank to a file with the public ip address as the file name
def write_rank_file(PublicIp, Rank):
    f = open(PublicIp, 'w')
    f.write(Rank)
    f.close()

def scp_node_list_to_all_ec2_instances(KeyPairName, insts):
    for i in range(len(insts)):
        print 'creating thread %s -> %s %s' % (i, KeyPairName, insts[i].id)
        t = threading.Thread(target=worker, \
                args=(KeyPairName, ec2.Instance(insts[i].id).public_ip_address))
        #threads.append(t)
        t.start()
#
#scp the nodelist file to all the ec2 instances multi threaded
#
def worker(KeyPairName, PublicIpAddr):
    print '%s -> scp -i ~/.ssh/%s -o StrictHostKeyChecking=no %s %s ec2-user@%s:%s' \
        % (threading.currentThread().getName(), \
            KeyPairName, 'nodelist', 'server.py', PublicIpAddr, '~/')
    #upload nodelist and server files
    os.system('scp -i ~/.ssh/%s -o StrictHostKeyChecking=no %s %s ec2-user@%s:%s'\
        % (KeyPairName, 'nodelist', 'server.py', PublicIpAddr, '~/') )
    #upload the rank to the node
    os.system('scp -i ~/.ssh/%s -o StrictHostKeyChecking=no %s ec2-user@%s:%s' \
        % (KeyPairName, PublicIpAddr, PublicIpAddr, '~/rank') )
       
#
#wait until running
#single thread model
#
def wait_for_all_instances(insts):
    for i in range(len(insts)):
        instance = ec2.Instance(insts[i].id)
        instance.wait_until_running() 
        print '"%s" started' % (instance.id)
        #if(ec2.Instance(instance.id).public_ip_address == None):
        print 'waiting for status checks to complete on instance "%s"' % (instance.id)
        waiter.wait(InstanceIds=[instance.id])
        print '%s status is OK' % (instance.id)


#
#multi-threaded model
#wait until all instances are running and status checks ok
#
def wait_for_all_instances_threaded(insts):
    for i in range(len(insts)):
        instance = ec2.Instance(insts[i].id)
        t = threading.Thread(target=worker_wait_for_start, \
             args=(instance, instance.id, instance.public_ip_address))
        t.start()
        t.join()
        

#
#worker thread to wait for all instances in a separate thread
#
def worker_wait_for_start(instance, Id, PublicIp):
    threadName = threading.currentThread().getName()
    print '%s -> waiting for instance %s to start...' \
        % (threadName, Id)
    instance.wait_until_running() 
    print '%s -> %s started' % (threadName, Id)
    #if(PublicIp == None):
    print '%s -> waiting for status checks to complete on instance "%s"' % (threadName, Id)
    waiter.wait(InstanceIds=[Id])    
    print '%s -> %s status is OK' % (threadName, instance.id)

#
#create list of instance ids
#
def create_list_of_instance_ids(insts):
    ids = []
    for i in range(len(insts)):
        ids[i] = insts[i].id
    return ids

#
# Process that starts the whole shebang!
#
def start_ec2_servers():
    #SecurityGroup = 'psim_security_group'
    #KeyPairName = 'amazonEC2pair'
    #KeyPairFile = 'amazonEC2pair.pem'
    print 'Creating the instances...'
    instances = create_instances_with_params(SecurityGroup, KeyPairName, 1)
    print 'Waiting for all instances to start...'
    wait_for_all_instances_threaded(instances)
    print 'Saving the node list to a file...'
    write_node_list(instances)
    print 'Copying files to each instance...'
    scp_node_list_to_all_ec2_instances(KeyPairFile, instances)


# 
#Read the nodelist file into a dictionary
#{node_num, private_ip_address}
#
def read_node_list():
    with open('nodelist', 'r') as f:
        for line in f:
            splitLine = shlex.split(line)
            PrivateIpNodeDictionary[int(splitLine[1])] = splitLine[0]


def read_file_tmp():
    with open('nodelist', 'r') as f:
        num = 0
        for line in f:
            num += 1
            print 'line %s ... value=%s' % (num, line)


#security_group = create_security_group()
#configure_security_group(security_group)


Options = 'f:h'

opts = getopt.getopt(sys.argv[2:], Options)[0]

for o in opts:
    if o[0] == '-f':
      KeyPairFile = o[1]
      KeyPairName = keyPairFile[:-4]
    elif o[0] == '-h':
      printUsage()
      sys.exit(0)
"""
BadUsage = False
if not Username:
    BadUsage = True
    print '-f is required'

if BadUsage:
    printUsage()
    sys.exit(1)
"""


