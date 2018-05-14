import getpass
import os
import time
import platform
import json
from timeit import default_timer
import base64

import click
import boto3


@click.group()
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool to benchmark a machine
    """
    ctx.obj = kwargs


@cli.command()
@click.option('--benchmark', type=click.Choice(['hibench', 'spark-perf']))
@click.option('--workload')
@click.option('--datasize')
@click.option('--iteration')
@click.option('--instance-type', default='m4.large')
@click.option('--keyname', default='osr')
@click.option('--ami', default='ami-9a24c8e0')
@click.option('--iam-instance-profile', default='arn:aws:iam::169987671570:instance-profile/chin')
@click.option('--volume-size', default=120)
@click.option('--volume-type', default='gp2')
@click.option('--subnet', default='subnet-54f22c30')
@click.option('--security-group', default='sg-42e01731')
@click.option('--availability-zone', default='us-east-1e')
@click.option('--user-data', default=None)
@click.option('--spot-price', default=None)
@click.option('--dry-run/--no-dry-run', default=False)
@click.option('--terminate/--no-terminate', default=True)
@click.pass_context
def run(ctx, *args, **kwargs):
    client = boto3.client('ec2')
    kwargs['user_data'] = _generate_launch_script(kwargs['benchmark'], kwargs['workload'], kwargs['datasize'], kwargs['iteration'], kwargs['terminate']) if kwargs['user_data'] is None else kwargs['user_data']
    kwargs['spot_price'] = _get_spot_price(kwargs['instance_type']) if kwargs['spot_price'] is None else kwargs['spot_price']
    print(kwargs['user_data'])
    print(base64.b64encode(kwargs['user_data'].encode()).decode())
    _request_spot_instance(client, **kwargs)


def _generate_launch_script(benchmark, workload, datasize, iteration, terminate=True):
    benchmark_program = 'auto_sparkperf.sh' if benchmark == 'spark-perf' else 'auto_hibench.sh'
    launch_script = '''#!/bin/bash -ex
mybenchmark()
{''' + '''
    su - osr /bin/bash -c 'cd /home/osr/project-aws; git pull; /bin/bash scripts/{} "{}" "{}" {}' '''.format(benchmark_program, workload, datasize, iteration) + '''
}

echo 'Executing the launch script' |& tee -a /tmp/init.out
mybenchmark |& tee -a /tmp/launch.out
echo 'Executed the launch script' |& tee -a /tmp/init.out
'''

    if terminate:
        launch_script += "\nshutdown -h now"

    return launch_script


def _get_spot_price(instance_type):
    price_table = {
        'c3.large': 0.032,
        'c3.xlarge': 0.086,
        'c3.2xlarge': 0.212,
        'c4.large': 0.045,
        'c4.xlarge': 0.092,
        'c4.2xlarge': 0.17,
        'm3.large': 0.054,
        'm3.xlarge': 0.096,
        'm3.2xlarge': 0.234,
        'm4.large': 0.044,
        'm4.xlarge': 0.105,
        'm4.2xlarge': 0.205,
        'm3.large': 0.061,
        'm3.xlarge': 0.081,
        'm3.2xlarge': 0.328,
        'r4.large': 0.031,
        'r4.xlarge': 0.067,
        'r4.2xlarge': 0.14,
    }
    return str(price_table[instance_type])


def _request_spot_instance(client, **kwargs):
    # http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.request_spot_instances

    response = client.request_spot_instances(
        DryRun=kwargs['dry_run'],
        InstanceCount=1,
        LaunchSpecification={
            'BlockDeviceMappings': [
                {
                    'DeviceName': '/dev/sda1',
                    'Ebs': {
                        'DeleteOnTermination': True,
                        'VolumeSize': kwargs['volume_size'],
                        'VolumeType': kwargs['volume_type']
                    },
                },
            ],
            'EbsOptimized': all([i not in kwargs['instance_type'] for i in ['c3', 'r3', 'm3', 't2']]),
            'IamInstanceProfile': {
                'Arn': kwargs['iam_instance_profile'],
            },
            'ImageId': kwargs['ami'],
            'InstanceType': kwargs['instance_type'],
            'KeyName': kwargs['keyname'],
            'NetworkInterfaces': [
                {
                    'AssociatePublicIpAddress': True,
                    'DeleteOnTermination': True,
                    'DeviceIndex': 0,
                    'Groups': [
                        kwargs['security_group'],
                    ],
                    'SubnetId': kwargs['subnet']
                },
            ],
            'Placement': {
                'AvailabilityZone': kwargs['availability_zone'],
            },
            'UserData': base64.b64encode(kwargs['user_data'].encode()).decode()
        },
        SpotPrice=kwargs['spot_price'],
        Type='one-time'
    )
