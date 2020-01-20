#!/usr/bin/env python

import argparse
from pathlib import Path
import sys
import os
import boto3
import time

project_path = Path.cwd().parent
script_path = project_path / 'script'
secrets_path = project_path / 'secrets'
sys.path.append(script_path)


def create_volume(availability_zone):
    client = boto3.client('ec2', region_name='us-east-1')
    ebs_vol = client.create_volume(Size=20, AvailabilityZone=availability_zone)

    # check that the EBS volume has been created successfully
    if ebs_vol['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("Successfully created Volume! " + ebs_vol['VolumeId'])
    return ebs_vol['VolumeId']


def attach_volume(volume_id, instance_id, first_time=False):
    client = boto3.client('ec2', region_name='us-east-1')
    public_dns_instance = client.describe_instances(InstanceIds=[instance_id])['Reservations'][0] \
        ['Instances'][0]['NetworkInterfaces'][0]['Association']['PublicDnsName']
    client.attach_volume(VolumeId=volume_id, InstanceId=instance_id,
                         Device='/dev/sdm')

    os.system("ssh -o StrictHostKeyChecking=no -i {} hadoop@{} sudo mkdir -p /cassandra/data".format(
        secrets_path / 'gdeltKeyPair-educate.pem',
        public_dns_instance))

    if first_time:
        os.system("ssh -o StrictHostKeyChecking=no -i {} hadoop@{} sudo mkfs -t ext4 /dev/sdm".format(
            secrets_path / 'gdeltKeyPair-educate.pem',
            public_dns_instance))
    os.system("ssh -o StrictHostKeyChecking=no -i {} hadoop@{} sudo mount /dev/sdm /cassandra/data".format(
        secrets_path / 'gdeltKeyPair-educate.pem',
        public_dns_instance))
    print(f"Successfully attached {volume_id} volume to {instance_id} instance!")
    return


def run_cassandra_container(instance_id, first_node=None):
    client = boto3.client('ec2', region_name='us-east-1')
    public_dns_instance = client.describe_instances(InstanceIds=[instance_id])['Reservations'][0] \
        ['Instances'][0]['NetworkInterfaces'][0]['Association']['PublicDnsName']
    private_ip_address = client.describe_instances(InstanceIds=[instance_id])['Reservations'][0] \
        ['Instances'][0]['NetworkInterfaces'][0]['PrivateIpAddress']
    if first_node is None:
        command = "ssh -o StrictHostKeyChecking=no -i {} hadoop@{} docker run --name cassandra-node -d -v /cassandra/data:/var/lib/cassandra -e CASSANDRA_BROADCAST_ADDRESS={} -p 7000:7000 -p 9042:9042 cassandra".format(
            secrets_path / 'gdeltKeyPair-educate.pem',
            public_dns_instance,
            private_ip_address)
        print('Run {}'.format(command))
        os.system(command)
    else:
        private_ip_address_first_node = client.describe_instances(InstanceIds=[first_node])['Reservations'][0] \
            ['Instances'][0]['NetworkInterfaces'][0]['PrivateIpAddress']
        command = "ssh -o StrictHostKeyChecking=no -i {} hadoop@{} docker run --name cassandra-node -d -v /cassandra/data:/var/lib/cassandra -e CASSANDRA_SEEDS={} -e CASSANDRA_BROADCAST_ADDRESS={} -p 7000:7000 -p 9042:9042 cassandra".format(
            secrets_path / 'gdeltKeyPair-educate.pem',
            public_dns_instance,
            private_ip_address_first_node,
            private_ip_address)
        print('Run {}'.format(command))
        os.system(command)
    print(f"Succesfully run cassandra container on the {instance_id} instance!")


if __name__ == '__main__':
    playbook_file = dict(spark=str('spark.yml'), cassandra=str('cassandra.yml'))
    parser = argparse.ArgumentParser()
    parser.add_argument('params', nargs='*', help='list of parameters')
    parser.add_argument('--create_cluster', dest='create_cluster', action='store_true',
                        help='Create a Spark or a Cassandra cluster depending on the given parameter : \
                        spark or cassandra')
    parser.add_argument('--attach_volume', dest='attach_volume', action='store_true',
                        help='Attache the given volume to the given instance')
    parser.add_argument('--first_time', dest='first_time', action='store_true',
                        help='Must be set if this is the first volume attachment to format the file system')
    parser.add_argument('--create_volume', dest='create_volume', action='store_true',
                        help='Must provide two parameter : number of volume to create and availability zone.\
                         Create x volume with x corresponding to the given parameter')
    parser.add_argument('--deploy_cassandra', dest='deploy_cassandra', action='store_true',
                        help='Must provide a list of instance id: Launch a cassandra container on each\
                         specify instances')
    args = parser.parse_args()

    if args.create_cluster:
        cluster_type = args.params.pop()
        os.system(f'ansible-playbook {playbook_file[cluster_type]}')
    elif args.attach_volume:
        instance_id = args.params[0]
        volume_id = args.params[1]
        attach_volume(volume_id=volume_id, instance_id=instance_id, first_time=args.first_time)
    elif args.create_volume:
        n_volumes = int(args.params[0])
        availability_zone = args.params[1]
        for i in range(n_volumes):
            create_volume(availability_zone)
    elif args.deploy_cassandra:
        first_node = args.params.pop()
        run_cassandra_container(instance_id=first_node)
        time.sleep(20)
        for instance_id in args.params:
            run_cassandra_container(instance_id, first_node)
