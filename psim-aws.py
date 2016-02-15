#!/usr/bin/env python

import boto.ec2
import os

instances = None
publicIps = None

#use the default configuration in .aws/config and .aws/credentials files
ec2 = boto3.resource('ec2')
#amazon linux - NOT Available
#ec2.create_instances(ImageId='ami-f0091d91', InstanceType='t1.micro', MinCount=1, MaxCount=1, DryRun=True)
#ami-d93622b8 - available
ec2.create_instances(ImageId='ami-d93622b8', InstanceType='t1.micro', MinCount=1, MaxCount=2
	, KeyName='amazonEC2pair'
    , SecurityGroups=[
        'psim_security_group',
    	])

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
def create_instances_with_params(SecurityGroupName, KeyNameString):
	instances = ec2.create_instances(ImageId='ami-d93622b8', InstanceType='t1.micro', MinCount=1, MaxCount=1
		, DryRun=True
		, KeyName=KeyNameString
    	, SecurityGroups=[
        	SecurityGroupName,
    	])	


def tag_all_instances():


def private_ips():

#
#Write the node list to a file
#node number and private ip address
#
def write_node_list(insts):
	f = open('nodelist','w')
	for i in range(len(insts)):
		f.write(str(i))
		f.write(' ')
		f.write(insts[i].private_ip_address)
		f.write('\n')
	
	f.close()

#
#scp the nodelist file to all the ec2 instances.  
#
def scp_node_list_to_all_ec2_instances(KeyPairName, insts):
	for i in range(len(insts)):
		os.system('scp -i ~/.ssh/' + KeyPairName + ' -o StrictHostKeyChecking=no "%s" "%s" "%s:%s"' 
			% ('nodelist', 'server.py', 'ec2-user@' + insts[i].public_ip_address, '~/') )


security_group = create_security_group()
configure_security_group(security_group)
