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

__Launch the platform__

```shell script
$ cd GDELT-Explore/script
$ ansible-playbook cassandra_launcher.yml
$ ansible-playbook spark_launcher.yml
```

Wait until the clusters are created.

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