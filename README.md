# psim-aws

#Amazon AWS
You must have an Amazon AWS account.

	http://aws.amazon.com/


You must create an Amazon AWS key pair.

	http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html


The private key must be saved in your default user directory in a .pem format

	~/.ssh/privatekey.pem


##Amazon AWS Configuration
You must enter you AWS security account credentials into the credentials file
to allow programmatic access to your Amazon AWS environment.

	http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html

Enter your credentials into the install/credentials file before you continue to the next step.


## Installation of dependencies
- pip - python package manager
- boto - aws python sdk original, superceded by boto3
- boto3 - aws python sdk
To install these dependencies run the following command:


	python install-boto.py


## Installation of psim-aws

These tools are a Python package like many others. To install them, run:

    python setup.py install
    
On a Mac or Linux you may have to use `sudo` to run this so that the tools
are installed in a system directory like `/usr/local/bin` with teh command below.


    sudo python setup.py install


After executing the following command you will be able to run the following command:

	psim-aws -h


prints:
	Usage: psim-aws -f [name] -h

	-f Amazon private key (.pem) located in ~/.ssh/key.pem
	-p Python file
	-t number of amazon EC2 instances to start
	-h Print this message