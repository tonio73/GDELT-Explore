# AWS project - GDELT dataset

## Prerequisites

You need to install the following dependencies before the creation of the platform
- [aws2 cli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux-mac.html)
- [ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html)

## Launch the aws architecture

__Configure aws2 cli__

First, aws2 cli needs to know your credentials to communicate with aws services through your account. By creating a
credential file in the `~/.aws` folder, aws2 cli will be able to interact with the platform :

```shell script
$ vim ~/.aws/credentials
```

Then copy-past the following lines with your keys instead of `X`:

```shell script
[default]
aws_access_key_id=XXXXXXXXXX
aws_secret_access_key=XXXXXXXXXXX
aws_session_token=XXXXXXXXXX
```

__Clone the project and add your .pem file__

gdeltKeyPair-educate.pem for the name of the pem file is mandatory.

```shell script
$ git clone https://github.com/tonio73/GDELT-Explore.git
$ mkdir GDELT-Explore/secrets && cp [path_to_pem] secrets/gdeltKeyPair-educate.pem
```

__Use the gdelt cli__

```shell script
$ cd GDELT-Explore/
$ pip install -r requirements.txt
```

In the `python/src` folder, there is a `gdelt.py` file which contains the cli for the project. This cli provide options 
to:
- Create the ec2 instances for the cluster
- Create the EBS volumes make cassandra data persistent
- Attach a volume to an ec2 instance
- Deploy a cassandra container on many ec2 instances

To get some help, run the following command:

````shell script
$ python gdelt.py --help
````
Create a cassandra cluster:

````shell script
$ python gdelt.py --create_cluster cassandra
````
Create the volumes:

````shell script
$ python gdelt.py --create_volume 3 [availability zone of the cluster]
````
Attach a volume (A volume need to be format when you use it for the first time):

````shell script
$ python gdelt.py --attach_volume --first_time [instance_id] [volume_id]
````

Deploy cassandra nodes

````shell script
$ python gdelt.py --deploy_cassandra [instance_id_1] [instance_id_2] ... [instance_id_n]
````

## Connect to the cassandra cluster

__Cqlsh__

You can access to the cassandra cluster with the console:

```shell script
$ ssh -i [path_to_pem] hadoop@[public_dns_instance]
```

Once connected to the instance, enter in the docker node to run the cqlsh console:

```shell script
$ docker exec -it cassandra-node bash
$ cqlsh
```

You can also check the status of the different cluster nodes:

```shell script
$ docker exec -it cassandra-node bash
$ nodetool status
```

## Connect to the spark cluster

You have two ways to access to the cluster:

- Zeppelin
- spark-shell