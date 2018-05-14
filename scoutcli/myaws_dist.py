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
@click.option('-w', '--workload', multiple=True, help="benchmark framework application datasize run_id, e.g., hibench hadoop terasort large 1")
@click.option('--instance-num', default=2)
@click.option('--instance-type', default='m4.large')
@click.option('--keyname', default='osr')
@click.option('--ami', default='ami-2fefa755')
@click.option('--iam-fleet-role', default='arn:aws:iam::169987671570:role/aws-ec2-spot-fleet-tagging-role')
@click.option('--iam-instance-profile', default='arn:aws:iam::169987671570:instance-profile/chin')
@click.option('--volume-size', default=120)
@click.option('--volume-type', default='gp2')
@click.option('--subnet', default='subnet-5421de6b')
@click.option('--security-group', default='sg-42e01731')
@click.option('--availability-zone', default='us-east-1e')
@click.option('--user-data', default=None)
@click.option('--spot-price', default=None)
@click.option('--cluster-mode', default='n+1')
@click.option('--dry-run/--no-dry-run', default=False)
@click.option('--terminate/--no-terminate', default=True)
@click.pass_context
def run(ctx, *args, **kwargs):
    # mybenchmark_v1: ami-44dae052
    # mybenchmark_v2: ami-d1688bab
    # mybehchmark_v3: ami-1ab65460
    # mybenchmark_v4: ami-3b2fc341
    # mybenchmark_v5: ami-9a24c8e0
    client = boto3.client('ec2')
    #print(json.dumps(kwargs))
    kwargs['user_data'] = _generate_launch_script(kwargs['workload'], kwargs['terminate']) if kwargs['user_data'] is None else kwargs['user_data']
    kwargs['spot_price'] = _get_spot_price(kwargs['instance_type']) if kwargs['spot_price'] is None else kwargs['spot_price']
    print(kwargs['user_data'])
    print(base64.b64encode(kwargs['user_data'].encode()).decode())
    #print(kwargs['spot_price'])
    _request_spot_instance(client, **kwargs)


def _generate_launch_script(workload_list, terminate=True):
    workload_str = " ".join(['"{}"'.format(workload) for workload in workload_list])
    launch_script = '''#!/bin/bash -ex
mybenchmark()
{''' + '''
    su - osr /bin/bash -c 'cd /home/osr/project-aws; git pull; /bin/bash scripts/auto_benchmark_dist.sh {}' '''.format(workload_str) + '''
}

echo 'Executing the launch script' |& tee -a /tmp/init.out
mybenchmark |& tee -a /tmp/launch.out
echo 'Executed the launch script' |& tee -a /tmp/init.out
'''

    if terminate:
        launch_script += "\nsu - osr /bin/bash -c 'cd /home/osr/project-aws; /bin/bash scripts/terminate_fleet.sh'"

    return launch_script


def _get_spot_price(instance_type):
    price_table = {
        'c3.large': 0.032,
        'c3.xlarge': 0.086,
        'c3.2xlarge': 0.212,
        'c4.large': 0.045,
        'c4.xlarge': 0.092,
        'c4.2xlarge': 0.17,
        'm3.large': 0.061,
        'm3.xlarge': 0.12,
        'm3.2xlarge': 0.26,
        'm4.large': 0.044,
        'm4.xlarge': 0.105,
        'm4.2xlarge': 0.205,
        'r3.large': 0.061,
        'r3.xlarge': 0.081,
        'r3.2xlarge': 0.328,
        'r4.large': 0.062,
        'r4.xlarge': 0.12,
        'r4.2xlarge': 0.26,
    }
    return str(price_table[instance_type])


def _request_spot_instance(client, **kwargs):
    # http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.request_spot_instances
    # https://github.com/boto/boto3/issues/714

    spot_fleet_request_config = {
        'TargetCapacity': kwargs['instance_num'],
        'SpotPrice': kwargs['spot_price'],
        'AllocationStrategy': 'lowestPrice',
        'IamFleetRole': kwargs['iam_fleet_role'],
        'LaunchSpecifications': [
            {
                'ImageId': kwargs['ami'],
                'KeyName': kwargs['keyname'],
                'InstanceType': kwargs['instance_type'],
                'IamInstanceProfile': {
                    'Arn': kwargs['iam_instance_profile'],
                },
                'EbsOptimized': all([i not in kwargs['instance_type'] for i in ['c3', 'r3', 'm3', 't2']]),
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
                'UserData': base64.b64encode(kwargs['user_data'].encode()).decode(),
                'Placement': { 'AvailabilityZone': kwargs['availability_zone'] },
                "TagSpecifications": [
                    {
                        'ResourceType': "instance",
                        "Tags": [
                            {
                                "Key": "cluster-mode",
                                "Value": str(kwargs['cluster_mode'])
                            },
                            {
                                "Key": "cluster-size",
                                "Value": str(kwargs['instance_num'])
                            }]
                    }
                ]
            },
        ],
    }

    print(json.dumps(spot_fleet_request_config, indent=True, sort_keys=True))
    response = client.request_spot_fleet(
        DryRun=kwargs['dry_run'],
        SpotFleetRequestConfig=spot_fleet_request_config
    )
