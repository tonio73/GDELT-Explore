# AWS project - GDELT dataset

[TOC]

# Infrastructure

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
Create a Cassandra cluster:

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

Deploy Cassandra nodes

````shell script
$ python gdelt.py --deploy_cassandra [instance_id_1] [instance_id_2] ... [instance_id_n]
````

## Connect to the cassandra cluster

__CQLSH__

You can access to the Cassandra cluster with the console:

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
- spark-submit

# Extract-Load-Transform (ETL)

The ETL is split into two parts:

1. Download the data from GDELT to an S3 storage
2. Transform and load to Cassandra the required views

First part is 1 program. The second part is split into 4 programs corresponding to the 4 queries specified in the project goals.

All 5 ETL programs are in the Scala SBT project.

## Pre-requisite

- Scala Build Tool (SBT)
- IntelliJ IDE
- Git

All the JAR dependencies are installed automatically through SBT

## Install the project

1. With Git, clone the project from the Github source repository
2. Open the project folder with IntelliJ
3. When first loading the project IntelliJ, the SBT files shall be imported
4. When pulling source code update from Github, it might be required to reload the SBT file

## Build ETL programs

Either :

- Through IntelliJ build

- Or with the command: 

  ```shell
  sbt assembly
  ```

- Or:

  ```shell
  ./build_and_submit.sh <programName>
  ```

  

## Download GDELT data

Create a folder /tmp/data (**LATER : use S3**)

Run the MainDownload program from the IDE or using spark-submit : 

```shell
spark-submit --class fr.telecom.MainDownload target/scala-2.11/GDELT-Explore-assembly-*.jar
```

Command line options :

- **--index** : download the masterfile indexes first

The reference period for the download is set in Scala object fr.telecom.Context

## Query A

### ETL for query A

From the shell using spark-submit:

```shell
spark-submit --class fr.telecom.MainQueryA target/scala-2.11/GDELT-Explore-assembly-*.jar
```

**LATER: add IP of Cassandra server !**

### Query A in Cassandra

```sql
CCC
```

