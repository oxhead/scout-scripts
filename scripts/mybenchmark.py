import boto3
import datetime
import json
import random

from executor import execute
import numpy as np

def main(workload_groups, datasize_list, instance_type_list, availability_zone_list, subnet_list, iteration, dry_run):
    spot_candidates = filter_spot_price(get_spot_price_history(instance_type_list, availability_zone_list))
    spot_biddings = create_spot_bidding(spot_candidates, factor=2)
    # print(json.dumps(spot_biddings, sort_keys=True, indent=True))

    count = 0
    for benchmark, workload_list in workload_groups:
        for instance_type in instance_type_list:
            availability_zone, price = random.choice(list(spot_biddings[instance_type].items()))
            #print(workload_list)
            #print('\t', instance_type)
            #print('\t\t', availability_zone, price)
            cmd = 'myaws run --benchmark {} --workload "{}" --datasize "{}" --iteration {} --instance-type {} --availability-zone {} --subnet {} --spot-price {} {}'.format(
                benchmark,
                " ".join(workload_list),
                " ".join(datasize_list),
                iteration,
                instance_type,
                availability_zone,
                subnet_list[availability_zone],
                price,
                '--dry-run' if dry_run else '--no-dry-run')
            print('echo {}'.format(cmd))
            print(cmd)
            print('sleep 15')
            #execute(cmd)
            count += 1
    # print('total instance:', count)



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
            spot_bidding_list[instance_type][availability_zone] = "{0:.3f}".format(price*2)
    return spot_bidding_list

if __name__ == '__main__':
    dry_run = False
    iteration = 3
    datasize_list = ['small', 'medium', 'large']
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

    # spark-perf
    # > 1200
    # regression, classification, gradient-boosted-tree, kmeans, naive-bayes, prefix-span, svd
    # > 500
    # als, chi-sq-feature, gmm, lda, pca, pearson, pic, word2vec
    # < 500
    # block-matrix-mult, decision-tree, random-forest, spearman, summary-statistics, fp-growth, chi-sq-gof, chi-sq-mat
    sparkperf_workload_groups = [
        ['regression', 'als', 'fp-growth', 'block-matrix-mult'],
        ['classification', 'chi-sq-feature', 'pic'],
        ['gradient-boosted-tree', 'gmm', 'decision-tree', 'random-forest'],
        ['kmeans', 'lda', 'chi-sq-gof', 'chi-sq-mat'],
        ['naive-bayes', 'pca', 'summary-statistics'],
        ['svd', 'pearson', 'word2vec', 'spearman'],
        # ['prefix-span'],  # not stable
    ]

    # hibench
    # > 1200
    # bayes.spark, lr.spark, pagerank.spark, pagerank.hadoop, sort.hadoop, terasort.hadoop
    # > 500
    # dfsioe.hadoop, sort.spark, terasort.spark
    # < 500
    # als.spark, wordcount.spark, aggregation.spark, aggregation.hadoop, join.spark, join.hadoop

    hibench_workload_groups = [
        ['bayes.spark', 'dfsioe.hadoop', 'wordcount.spark'],
        ['lr.spark', 'als.spark', 'aggregation.spark', 'aggregation.hadoop'],
        ['pagerank.spark', 'join.spark', 'join.hadoop'],
        ['pagerank.hadoop', 'scan.spark', 'scan.hadoop'],
        ['sort.hadoop', 'sort.spark'],
        ['terasort.hadoop', 'terasort.spark']
    ]

    workload_groups = []
    #for workload_list in sparkperf_workload_groups:
    #    workload_groups.append(('spark-perf', workload_list))
    for workload_list in hibench_workload_groups:
        workload_groups.append(('hibench', workload_list))

    main(workload_groups, datasize_list, instance_type_list, availability_zone_list, subnet_list, iteration, dry_run)
    #price_history = get_spot_price_history(instance_type_list, availability_zone_list)
    #print('before')
    #print(json.dumps(price_history, indent=True, sort_keys=True))
    #print('after')
    #print(json.dumps(filter_spot_price(price_history), indent=True, sort_keys=True))
