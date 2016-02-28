#!/usr/bin/env python

import os
#import urllib2
#import wget

#response = urllib2.urlopen('https://bootstrap.pypa.io/get-pip.py')
#html = response.read()


#url = 'https://bootstrap.pypa.io/get-pip.py'
#filename = wget.download(url, install/get-pip.py)
os.system('sudo python install/get-pip.py')
os.system('sudo pip install wget boto')
os.system('sudo pip install --ignore-installed six boto3')

os.system('mkdir ~/.aws')
os.system('cp install/config ~/.aws/config')
os.system('cp install/credentials ~/.aws/credentials')

