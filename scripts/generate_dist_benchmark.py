import itertools
import random

import numpy as np
import boto3


def get_spot_price_history(instance_type_list, avaiability_zone_list, region_name='us-east-1'):
    client = boto3.client('ec2', region_name=region_name)
    price_history = {}
    for instance_type in instance_type_list:
        price_history[instance_type] = {}
        for avaiability_zone in avaiability_zone_list:

            prices = client.describe_spot_price_history(
                InstanceTypes=[instance_type],
                AvailabilityZone=avaiability_zone,
                MaxResults=1000,
                ProductDescriptions=['Linux/UNIX']
            )

            price_list = [float(record['SpotPrice']) for record in prices['SpotPriceHistory']]
            price_history[instance_type][avaiability_zone] = sum(price_list) / len(price_list)
    return price_history


def filter_spot_price(price_history, percentile=50):
    price_list = {}
    for instance_type in price_history.keys():
        price_list[instance_type] = {}
        threshold = np.percentile(list(price_history[instance_type].values()), percentile)
        for availability_zone, price in price_history[instance_type].items():
            if price <= threshold:
                price_list[instance_type][availability_zone] = price
    return price_list


def create_spot_bidding(spot_candidates, factor=2):
    spot_bidding_list = {}
    for instance_type in spot_candidates.keys():
        spot_bidding_list[instance_type] = {}
        for availability_zone, price in spot_candidates[instance_type].items():
            spot_bidding_list[instance_type][availability_zone] = "{0:.3f}".format(price*factor)
    return spot_bidding_list


def main():
    instance_type_list = ['c3.large', 'c3.xlarge', 'c3.2xlarge',
                          'c4.large', 'c4.xlarge', 'c4.2xlarge',
                          'm3.large', 'm3.xlarge', 'm3.2xlarge',
                          'm4.large', 'm4.xlarge', 'm4.2xlarge',
                          'r3.large', 'r3.xlarge', 'r3.2xlarge',
                          'r4.large', 'r4.xlarge', 'r4.2xlarge']
    availability_zone_list = ['us-east-1a', 'us-east-1b', 'us-east-1c', 'us-east-1d', 'us-east-1e', 'us-east-1f']
    subnet_list = {
        'us-east-1a': 'subnet-41634a1b',
        'us-east-1b': 'subnet-f947e39d',
        'us-east-1c': 'subnet-818bbead',
        'us-east-1d': 'subnet-c1d3268a',
        'us-east-1e': 'subnet-5421de6b',
        'us-east-1f': 'subnet-1581dd19',
    }

    bid_factor = 2
    spot_candidates = filter_spot_price(get_spot_price_history(instance_type_list, availability_zone_list))
    spot_biddings = create_spot_bidding(spot_candidates, factor=bid_factor)

    app_list = [
        #('hibench', 'hadoop', 'wordcount'),
        #('sparkperf', 'spark1.5', 'regression'),
        #('hibench', 'hadoop', 'terasort'),
        #('hibench', 'spark', 'join'),
        #('sparkperf', 'spark1.5', 'naive-bayes'),
        #('hibench', 'hadoop', 'pagerank'),
        ('hibench', 'spark', 'lr'),
        #('sparkperf', 'spark1.5', 'kmeans'),
        #('hibench', 'spark', 'pagerank'),
    ]

    datasize_list = ['huge', 'bigdata']
    #datasize_list = ['huge']
    run_id_list = [1]
    instance_family_list = ['c4', 'm4', 'r4']
    deployment_list = [
        (4, 'large'), (4, 'xlarge'), (4, '2xlarge'),
        (6, 'large'), (6, 'xlarge'), (6, '2xlarge'),
        (8, 'large'), (8, 'xlarge'), (8, '2xlarge'),
        (10, 'large'), (10, 'xlarge'), (10, '2xlarge'),
        (12, 'large'), (12, 'xlarge'), (12, '2xlarge'),
        (16, 'large'), (16, 'xlarge'),
        (24, 'large'), (20, 'xlarge'),
        (32, 'large'), (24, 'xlarge'),
        (40, 'large'),
        (48, 'large'),
    ]
    price_table = {
        'c4.large': 0.028,
        'c4.xlarge': 0.057,
        'c4.2xlarge': 0.115,
        'm4.large': 0.03,
        'm4.xlarge': 0.061,
        'm4.2xlarge': 0.13,
        'r4.large': 0.032,
        'r4.xlarge': 0.065,
        'r4.2xlarge': 0.135, 
    }

    count = 0
    cost = 0
    for deployment in deployment_list:
        instance_num = deployment[0] + 1  # n+1 mode
        if deployment[0] < 8 or deployment[0] > 8:
            continue
        for instance_family in instance_family_list:
            if instance_family != 'c4':
                continue

            if deployment[1] != '2xlarge':
                continue

            instance_type = '{}.{}'.format(instance_family, deployment[1])
            availability_zone, price = random.choice(list(spot_biddings[instance_type].items()))
            subnet = subnet_list[availability_zone]

            cmd = 'myaws_dist run'
            for (datasize, app, run_id) in itertools.product(datasize_list, app_list, run_id_list):
                workload = '{} {} {} {} {}'.format(app[0], app[1], app[2], datasize, run_id) 
                cmd += ' -w "{}"'.format(workload)
                cost += instance_num * (float(price) / bid_factor * 1.2) * 2

            cmd += ' --spot-price {}'.format(price)
            cmd += ' --availability-zone {}'.format(availability_zone)
            cmd += ' --subnet {}'.format(subnet)
            cmd += ' --instance-type {} --instance-num {} --terminate --dry-run'.format(instance_type, instance_num)
            print(cmd)
            count += instance_num
        print('')
    print('@@ count:', count)
    print('@@ cost:', cost)


if __name__ == '__main__':
    main()


# bash scripts/auto_benchmark_dist.sh "hibench hadoop wordcount huge 1" "sparkperf spark1.5 regression huge 1" "hibench hadoop terasort huge 1" "hibench spark join huge 1" "sparkperf spark1.5 naive-bayes huge 1" "hibench hadoop pagerank huge 1" "hibench spark lr huge 1" "sparkperf spark1.5 kmeans huge 1" "hibench spark pagerank huge 1" "hibench hadoop wordcount bigdata 1" "sparkperf spark1.5 regression bigdata 1" "hibench hadoop terasort bigdata 1" "hibench spark join bigdata 1" "sparkperf spark1.5 naive-bayes bigdata 1" "hibench hadoop pagerank bigdata 1" "hibench spark lr bigdata 1" "sparkperf spark1.5 kmeans bigdata 1" "hibench spark pagerank bigdata 1"
