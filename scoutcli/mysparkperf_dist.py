import getpass
import os
import time
import platform
import psutil
import json
from timeit import default_timer
import datetime

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from scout.util import helper
from scout.util import aws as aws_helper

import myhadoop_dist as myhadoop
from myhibench_dist import HiBenchClusterProfiler

@click.group()
@click.option('--sparkperf_dir', default="/home/osr/spark-perf", type=click.Path(exists=True, resolve_path=True))
@click.option('--hadoop_dir', default="/opt/hadoop", type=click.Path(exists=True, resolve_path=True))
@click.option('--spark_dir', default="/home/osr/spark-1.5", type=click.Path(exists=True, resolve_path=True))
@click.option('--monitoring', type=click.Path(exists=False, resolve_path=True))
@click.option('--interval', type=int, default=5)
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool to benchmark a machine
    """
    ctx.obj = kwargs


@cli.command()
@click.option('--master')
@click.option('--slaves')
@click.pass_context
def auto_configure(ctx, master, slaves):
    # step
    # 0. common setting
    slave_list = list(sorted(slaves.split(' ')))
    print(master)
    print(slave_list)
    hostname = aws_helper.Instance.get_private_ip()
    instance_type = aws_helper.Instance.get_instance_type()
    print("Instance Type:", instance_type)
    num_cores = aws_helper.Instance.get_num_of_cores()
    memory_size = ctx.invoke(get_memory, instance=instance_type)

    # 1. configure Hadoop, HDFS and Yarn
    am_cores = 1  # arbitrary
    task_cores = 1  # map/reduce task use one core per task
    memory_size = ctx.invoke(get_memory, instance=instance_type)
    ctx.invoke(myhadoop.configure,
               replicas=1,
               hostname=hostname,
               cores=num_cores,
               memory=memory_size,
               am_cores=am_cores,
               task_cores=task_cores,
               master=master,
               slaves=slaves
               )


@cli.command()
@click.option('--slaves')
@click.option('--mode')
@click.pass_context
def get_spark_env(ctx, slaves, mode):
    instance_type = aws_helper.Instance.get_instance_type()
    num_cores = aws_helper.Instance.get_num_of_cores()
    memory_size = ctx.invoke(get_memory, instance=instance_type)

    slave_list = list(sorted(slaves.split(' ')))

    # AppMaster
    am_cores = 1
    memory_per_core = int(memory_size / num_cores)
    am_memory_overhead = 512 if (am_cores * memory_per_core) <= 4096 else 1024
    # am_memory_overhead = 1024  # should be find for 2G to 8GB memory per core
    am_memory = int(am_cores * memory_per_core - am_memory_overhead)

    if mode == 'n+1':
        driver_local_memory = memory_size
    else:
        driver_local_memory = memory_per_core

    executor_cores = 1
    executor_num = int((len(slave_list) * num_cores - am_cores) / executor_cores)
    executor_memory_overhead = 512 if (executor_cores * memory_per_core) <= 4096 else 1024
    executor_memory = int(memory_per_core*executor_cores - executor_memory_overhead - executor_memory_overhead)
    map_parallelism = num_cores * len(slave_list) * 2
    reduce_parallelism = num_cores * len(slave_list) * 2

    env_settings = {
        #'spark.driver.memory': '{}m'.format(driver_memory),
        'spark.executor.instances': executor_num,
        'spark.executor.cores': executor_cores,
        'spark.executor.memory': '{}m'.format(executor_memory),
        'spark.driver.memory': '{}m'.format(driver_local_memory),
        'spark.yarn.driver.memoryOverhead': am_memory_overhead,
        'spark.yarn.executor.memoryOverhead': executor_memory_overhead,  # no unit, different from Spark 2.1
        'spark.yarn.am.cores': am_cores,
        'spark.yarn.am.memory': '{}m'.format(am_memory),
        'spark.yarn.am.memoryOverhead': am_memory_overhead,  # no unit, different from Spark 2.1
        'spark.storage.memoryFraction': 0.66,
        'spark.serializer': 'org.apache.spark.serializer.JavaSerializer',
        'spark.shuffle.manager': 'SORT',
        'spark.yarn.maxAppAttempts': 1,
        'spark.task.maxFailures': 1,  # for fair comparison among workloads
        'sparkperf.executor.num': executor_num,  # piggyback for num-partitions in spark-perf
    }
    return env_settings


@cli.command()
@click.pass_context
def start(ctx):
    ctx.invoke(myhadoop.start)


@cli.command()
@click.pass_context
def stop(ctx):
    ctx.invoke(myhadoop.stop)


@cli.command()
@click.pass_context
def init(ctx):
    ctx.invoke(myhadoop.stop)
    ctx.invoke(myhadoop.init)


@cli.command()
@click.pass_context
def clean(ctx):
    pass


@cli.command()
@click.option('--instance', default='c4.large')
@click.pass_context
def get_config_profile(ctx, instance):
    configure_profiles = {
        # the last one filed is for the Hadoop application. Not used in spark-perf
        # 'type.size': (am_driver_mem, am_driver_mem_overhead, executor_mem_overhead, hadoop_am_mem)
        'c3.large': (2048, 512, 512, 1024),
        'c3.xlarge': (2048, 512, 512, 1024),
        'c3.2xlarge': (2048, 512, 512, 1024),
        'c4.large': (2048, 512, 512, 1024),
        'c4.xlarge': (2048, 512, 512, 1024),
        'c4.2xlarge': (2048, 512, 512, 1024),
        'm3.large': (4096, 512, 512, 1024),
        'm3.xlarge': (4096, 512, 512, 1024),
        'm3.2xlarge': (4096, 512, 512, 1024),
        'm4.large': (4096, 512, 512, 1024),
        'm4.xlarge': (4096, 512, 512, 1024),
        'm4.2xlarge': (4096, 512, 512, 1024),
        'r3.large': (8192, 512, 512, 1024),
        'r3.xlarge': (8192, 512, 512, 1024),
        'r3.2xlarge': (8192, 512, 512, 1024),
        'r4.large': (8192, 512, 512, 1024),
        'r4.xlarge': (8192, 512, 512, 1024),
        'r4.2xlarge': (8192, 512, 512, 1024),
        't2.large': (4096, 512, 512, 1024),
        't2.xlarge': (4096, 512, 512, 1024),
        't2.2xlarge': (4096, 512, 512, 1024),
    }
    return configure_profiles[instance]


@cli.command()
@click.option('--instance', default='c4.large')
@click.pass_context
def get_memory(ctx, instance):
    # not accurate memory but used for HiBench
    memory_sizes = {
        'c3.large': 4096,
        'c3.xlarge': 8192,
        'c3.2xlarge': 16384,
        'c4.large': 4096,
        'c4.xlarge': 8192,
        'c4.2xlarge': 16384,
        'm3.large': 8192,
        'm3.xlarge': 16384,
        'm3.2xlarge': 32768,
        'm4.large': 8192,
        'm4.xlarge': 16384,
        'm4.2xlarge': 32768,
        'r3.large': 16384,
        'r3.xlarge': 32768,
        'r3.2xlarge': 65536,
        'r4.large': 16384,
        'r4.xlarge': 32768,
        'r4.2xlarge': 65536,
        't2.large': 8192,
        't2.xlarge': 16384,
        't2.2xlarge': 32768,
    }
    return memory_sizes[instance]


@cli.command()
@click.option('--workload', default='regression')
@click.option('--datasize', default='size1')
@click.option('--num_partitions', type=int, default=1)
@click.option('--output_dir', default=None, type=click.Path(exists=False, resolve_path=True))
@click.pass_context
def generate_command(ctx, workload, datasize, num_partitions, output_dir):
    # @TODO: configure num.partitions
    default_app_settings = {
        'master': 'yarn',
        'driver-memory': '1024m',
    }

    default_workload_settings = {
        'num-trials': 1,
        'inter-trial-wait': 1,
        'random-seed': 5,
        'num-partitions': num_partitions,
    }
    default_workload_table = {
        'regression': {
            'class': 'glm-regression',
            'num-iterations': 20,
            'feature-noise': 1.0,
            'step-size': 0.001,
            'reg-type': 'l2',
            'reg-param': 0.1,
            'elastic-net-param': 0.0,
            'optimizer': 'sgd',  # auto
            'intercept': 0.0,
            'label-noise': 0.1,
            'loss': 'l2',
        },
        'classification': {
            'class': 'glm-classification',
            'num-iterations': 20,
            'feature-noise': 1.0,
            'step-size': 0.001,
            'reg-type': 'l2',
            'reg-param': 0.1,
            'elastic-net-param': 0.0,
            'optimizer': 'sgd',  # l-bfgs
            'loss': 'logistic',
            'per-negative': 0.3,

        },
        'naive-bayes': {
            'class': 'naive-bayes',
            'feature-noise': 1,
            'per-negative': 0.3,
            'nb-lambda': 1,
            'model-type': 'multinomial',
        },
        'decision-tree': {
            'class': 'decision-tree',
            'label-type': 0,  # 2
            'frac-categorical-features': 0.5,
            'frac-binary-features': 0.5,
            'max-bins': 32,
            'ensemble-type': 'RandomForest',  # GradientBoostedTrees | ml.GradientBoostedTree
            'training-data': '',
            'test-data': '',
            'test-data-fraction': 0.2,
            'tree-depth': 10,  # 10,
            'num-trees': 1,  # 10,
            'feature-subset-strategy': 'auto'
        },
        'random-forest': {
            'class': 'decision-tree',
            'label-type': 0,  # 2
            'frac-categorical-features': 0.5,
            'frac-binary-features': 0.5,
            'max-bins': 32,
            'ensemble-type': 'RandomForest',  # GradientBoostedTrees | ml.GradientBoostedTree
            'training-data': '',
            'test-data': '',
            'test-data-fraction': 0.2,
            'tree-depth': 10,  # 10,
            'num-trees': 10,  # 10,
            'feature-subset-strategy': 'auto'
        },
        'gradient-boosted-tree': {
            'class': 'decision-tree',
            'label-type': 0,  # 2
            'frac-categorical-features': 0.5,
            'frac-binary-features': 0.5,
            'max-bins': 32,
            'ensemble-type': 'GradientBoostedTrees',  # GradientBoostedTrees | ml.GradientBoostedTree
            'training-data': '',
            'test-data': '',
            'test-data-fraction': 0.2,
            'tree-depth': 5,  # 10,
            'num-trees': 10,  # 10,
            'feature-subset-strategy': 'auto'
        },
        'als': {
            'class': 'als',
            'num-iterations': 10,
            'reg-param': 0.1,
            'rank': 10
        },
        'kmeans': {
            'class': 'kmeans',
            'num-iterations': 20,
            'num-centers': 20,
        },
        'gmm': {
            'class': 'gmm',
            'num-iterations': 20,
            'num-centers': 20,
        },
        'lda': {
            'class': 'lda',
            'optimizer': 'em',
            'num-iterations': 40,
            'num-topics': 20,
            'document-length': 100,
        },
        'pic': {
            'class': 'pic',
            'num-iterations': 20,
            'num-centers': 40,
            'node-degree': 20,
        },
        'svd': {
            'class': 'svd',
            'rank': 10,
        },
        'pca': {
            'class': 'pca',
            'rank': 50,
        },
        'summary-statistics': {
            'class': 'summary-statistics',
            'rank': 50,
        },
        'block-matrix-mult': {
            'class': 'block-matrix-mult',
            'block-size': 1024
        },
        'pearson': {
            'class': 'pearson',
        },
        'spearman': {
            'class': 'spearman',
        },
        'chi-sq-feature': {
            'class': 'chi-sq-feature',
        },
        'chi-sq-gof': {
            'class': 'chi-sq-gof',
        },
        'chi-sq-mat': {
            'class': 'chi-sq-mat',
        },
        'word2vec': {
            'class': 'word2vec',
            'num-iterations': 10,
            'vector-size': 100,
            'min-count': 5,
        },
        'fp-growth': {
            'class': 'fp-growth',
            'min-support': 0.01,
        },
        'prefix-span': {
            'class': 'prefix-span',
            'min-support': 0.5,
            'max-local-proj-db-size': 32000000,
            'avg-sequence-size': 50,
            'avg-itemset-size': 50,
            'max-pattern-len': 10,
        }

    }

    # size0 for warming up the system
    workload_table = {
        'regression': {
            'warmup': {
                'num-examples': 1000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 50000,
                'num-features': 10000,
            },
            'medium': {
                'num-examples': 100000,
                'num-features': 10000,
            },
            'large': {
                'num-examples': 100000,
                'num-features': 10000,
            },
            'huge': {
                'num-examples': 500000,
                'num-features': 25000,
            },
            'bigdata': {
                'num-examples': 1000000,
                'num-features': 50000,
            },
        },
        'classification': {
            'warmup': {
                'num-examples': 1000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 20000,
                'num-features': 10000,
            },
            'medium': {
                'num-examples': 40000,
                'num-features': 10000,
            },
            'large': {
                'num-examples': 80000,
                'num-features': 10000,
            },
        },
        'naive-bayes': {
            'warmup': {
                'num-examples': 10000,
                'num-features': 1000,
            },
            'small': {
                'num-examples': 100000,
                'num-features': 10000,
            },
            'medium': {
                'num-examples': 200000,
                'num-features': 10000,
            },
            'large': {
                'num-examples': 400000,
                'num-features': 10000,
            },
            'huge': {
                'num-examples': 1000000,
                'num-features': 25000,
            },
            'bigdata': {
                'num-examples': 1000000,
                'num-features': 50000,
            },
        },
        'decision-tree': {
            'warmup': {
                'num-examples': 100000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 200000,
                'num-features': 200,
            },
            'medium': {
                'num-examples': 400000,
                'num-features': 200,
            },
            'large': {
                'num-examples': 800000,
                'num-features': 200,
            },
        },
        'random-forest': {
            'warmup': {
                'num-examples': 100000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 400000,
                'num-features': 100,
            },
            'medium': {
                'num-examples': 600000,
                'num-features': 100,
            },
            'large': {
                'num-examples': 800000,
                'num-features': 100,
            },
        },
        'gradient-boosted-tree': {
            'warmup': {
                'num-examples': 10000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 10000,
                'num-features': 100,
            },
            'medium': {
                'num-examples': 20000,
                'num-features': 100,
            },
            'large': {
                'num-examples': 800000,
                'num-features': 100,
            },
        },
        'als': {
            'warmup': {
                'num-users': 10000,
                'num-products': 10000,
                'num-rating': 10000,
            },
            'small': {
                'num-users': 100000,
                'num-products': 500000,
                'num-rating': 1000000,
            },
            'medium': {
                'num-users': 100000,
                'num-products': 500000,
                'num-rating': 1500000,
            },
            'large': {
                'num-users': 200000,
                'num-products': 500000,
                'num-rating': 2000000,
            },
        },
        'kmeans': {
            'warmup': {
                'num-examples': 1000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 10000,
                'num-features': 10000,
            },
            'medium': {
                'num-examples': 25000,
                'num-features': 10000,
            },
            'large': {
                'num-examples': 50000,
                'num-features': 10000,
            },
            'huge': {
                'num-examples': 1000000,
                'num-features': 10000,
            },
            'bigdata': {
                'num-examples': 2000000,
                'num-features': 10000,
            },
        },
        'gmm': {
            'warmup': {
                'num-examples': 50000,
                'num-features': 100,
            },
            'small': {
                'num-examples': 100000,
                'num-features': 100,
            },
            'medium': {
                'num-examples': 200000,
                'num-features': 100,
            },
            'large': {
                'num-examples': 200000,
                'num-features': 200,
            },
        },
        'lda': {
            'warmup': {
                'num-documents': 2500,
                'num-vocab': 1000,
            },
            'small': {
                'num-documents': 10000,
                'num-vocab': 10000,
            },
            'medium': {
                'num-documents': 15000,
                'num-vocab': 10000,
            },
            'large': {
                'num-documents': 20000,
                'num-vocab': 10000,
            },
        },
        'pic': {
            'warmup': {
                'num-examples': 1000,
            },
            'small': {
                'num-examples': 50000,
            },
            'medium': {
                'num-examples': 100000,
            },
            'large': {
                'num-examples': 150000,
            },
        },
        'svd': {
            'warmup': {
                'num-rows': 50000,
                'num-cols': 500,
            },
            'small': {
                'num-rows': 100000,
                'num-cols': 500,
            },
            'medium': {
                'num-rows': 200000,
                'num-cols': 500,
            },
            'large': {
                'num-rows': 400000,
                'num-cols': 500,
            },
        },
        'pca': {
            'warmup': {
                'num-rows': 50000,
                'num-cols': 1000,
            },
            'small': {
                'num-rows': 100000,
                'num-cols': 1000,
            },
            'medium': {
                'num-rows': 200000,
                'num-cols': 1000,
            },
            'large': {
                'num-rows': 400000,
                'num-cols': 1000,
            },
        },
        'summary-statistics': {
            'warmup': {
                'num-rows': 50000,
                'num-cols': 500,
            },
            'small': {
                'num-rows': 100000,
                'num-cols': 1000,
            },
            'medium': {
                'num-rows': 200000,
                'num-cols': 2000,
            },
            'large': {
                'num-rows': 500000,
                'num-cols': 5000,
            },
        },
        # not working for some parameters
        'block-matrix-mult': {
            'warmup': {
                'm': 1000,
                'k': 10000,
                'n': 10000,
            },
            'small': {
                'm': 4000,
                'k': 10000,
                'n': 4000,
            },
            'medium': {
                'm': 4000,
                'k': 10000,
                'n': 10000,
            },
            'large': {
                'm': 5000,
                'k': 5000,
                'n': 5000,
            },
        },
        'pearson': {
            'warmup': {
                'num-rows': 50000,
                'num-cols': 500,
            },
            'small': {
                'num-rows': 100000,
                'num-cols': 1000,
            },
            'medium': {
                'num-rows': 200000,
                'num-cols': 1500,
            },
            'large': {
                'num-rows': 200000,
                'num-cols': 2000,
            },
        },
        'spearman': {
            'warmup': {
                'num-rows': 50000,
                'num-cols': 100,
            },
            'small': {
                'num-rows': 40000,
                'num-cols': 200,
            },
            'medium': {
                'num-rows': 80000,
                'num-cols': 200,
            },
            'large': {
                'num-rows': 60000,
                'num-cols': 200,
            }
        },
        'chi-sq-feature': {
            'warmup': {
                'num-rows': 100000,
                'num-cols': 500,
            },
            'small': {
                'num-rows': 500000,
                'num-cols': 1000,
            },
            'medium': {
                'num-rows': 1000000,
                'num-cols': 1000,
            },
            'large': {
                'num-rows': 2000000,
                'num-cols': 1000,
            },
            'huge': {
                'num-rows': 10000000,
                'num-cols': 1000,
            },
        },
        # not usable due to Java out of Heap space
        'chi-sq-gof': {
            'warmup': {
                'num-rows': 2500000,
                'num-cols': 0,
            },
            'small': {
                'num-rows': 5000000,
                'num-cols': 0,
            },
            'medium': {
                'num-rows': 1000000,
                'num-cols': 0,
            },
            'large': {
                'num-rows': 10000000,
                'num-cols': 0,
            },
        },
        # not usable due to Java out of Heap space
        'chi-sq-mat': {
            'warmup': {
                'num-rows': 1000,
                'num-cols': 10,
            },
            'small': {
                'num-rows': 100000,
                'num-cols': 0,
            },
            'medium': {
                'num-rows': 200000,
                'num-cols': 0,
            },
            'large': {
                'num-rows': 5000,
                'num-cols': 0,
            },
        },
        'word2vec': {
            'warmup': {
                'num-sentences': 50000,
                'num-words': 10000,
            },
            'small': {
                'num-sentences': 100000,
                'num-words': 10000,
            },
            'medium': {
                'num-sentences': 200000,
                'num-words': 10000,
            },
            'large': {
                'num-sentences': 400000,
                'num-words': 10000,
            },
            'huge': {
                'num-iterations': 10,
                'vector-size': 100,
                'min-count': 5,
                'num-sentences': 1000000,
                'num-words': 100000,
            }
        },
        'fp-growth': {
            'warmup': {
                'num-baskets': 1000,
                'num-items': 100,
                'avg-basket-size': 10,
            },
            'small': {
                'num-baskets': 40000,
                'num-items': 1000,
                'avg-basket-size': 10,
            },
            'medium': {
                'num-baskets': 60000,
                'num-items': 1000,
                'avg-basket-size': 10,
            },
            'large': {
                'num-baskets': 80000,
                'num-items': 1000,
                'avg-basket-size': 10,
            },
        },
        'prefix-span': {
            'warmup': {
                'num-items': 500,
                'num-sequences': 250000,
            },
            'small': {
                'num-items': 10000,
                'num-sequences': 500000,
            },
            'medium': {
                'num-items': 20000,
                'num-sequences': 500000,
            },
            'large': {
                'num-items': 40000,
                'num-sequences': 500000,
            },
        }

    }

    workload_setting = {}
    workload_setting.update(default_workload_settings)
    workload_setting.update(default_workload_table[workload])
    workload_setting.update(workload_table[workload][datasize])

    cmd_spark = "{}/bin/spark-submit --class mllib.perf.TestRunner".format(ctx.obj['spark_dir'])
    cmd_spark_params = "--class mllib.perf.TestRunner --master {master}".format(**default_app_settings)
    #cmd_spark_params = "--class mllib.perf.TestRunner --master {master} --driver-memory {driver-memory}".format(**default_app_settings)
    spark_program = '{}/mllib-tests/target/mllib-perf-tests-assembly.jar'.format(ctx.obj['sparkperf_dir'])
    workload_class = workload_setting.pop('class')
    cmd_workload_params = " ".join(['--{}={}'.format(k, workload_setting[k]) for k in workload_setting.keys()])
    stdout_file = os.path.join(output_dir, 'log.out')
    stderr_file = os.path.join(output_dir, 'log.err')
    cmd = "{} {} {} {} {} 1>> {} 2>>{}".format(
        cmd_spark, cmd_spark_params, spark_program, workload_class, cmd_workload_params,
        stdout_file, stderr_file)
    return cmd


@cli.command()
@click.option('-w', '--workload', help="workload.framework, e.g., wordcount.spark")
@click.option('--datasize', default='size1')
@click.option('--output_dir', default=None, type=click.Path(exists=False, resolve_path=True))
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.option('--timeout', type=int, default=60*60*24)
@click.option('--slaves')
@click.option('--mode')
@click.pass_context
def run(ctx, workload, datasize, output_dir, monitoring, interval, timeout, slaves, mode):
    execute("rm -rf {}; mkdir -p {}".format(output_dir, output_dir))

    # 2. prepare env setting?
    # @TODO: num-partitions?  x cores?
    spark_env = ctx.invoke(get_spark_env, slaves=slaves, mode=mode)
    env_settings = {
        'HADOOP_CONF_DIR': "{}/etc/hadoop".format(ctx.obj['hadoop_dir']),
        'SPARK_SUBMIT_OPTS': " ".join(["-D{}={}".format(k, spark_env[k]) for k in spark_env.keys()])
    }

    execute("export | grep HADOOP_CONF_DIR", environment=env_settings)
    execute("export | grep SPARK_SUBMIT_OPTS", environment=env_settings)

    # 1. generate commands
    cmd = ctx.invoke(generate_command, workload=workload, datasize=datasize, num_partitions=spark_env['sparkperf.executor.num'], output_dir=output_dir)
    cmd = "timeout {}s {}".format(timeout, cmd)
    print(cmd)

    timestampt = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    if monitoring:
        monitoring_output = os.path.join(output_dir, 'sar.csv')
        slave_list = list(sorted(slaves.split(' ')))
        with HiBenchClusterProfiler(slave_list, monitoring_output, interval) as app_profiler:
            with helper.Timer() as timer:
                successful = execute(cmd, environment=env_settings, check=False)
    else:
        with helper.Timer() as timer:
            successful = execute(cmd, environment=env_settings, check=False)

    report = {
        'workload': workload,
        'framework': 'spark1.5',
        'datasize': datasize,
        'completed': successful,
        'program': workload,
        'timestamp': timestampt,
        'input_size': -1,
        'elapsed_time': timer.elapsed_secs,
        'throughput_cluster': -1,
        'throughput_node': -1
    }

    report_json = os.path.join(output_dir, 'report.json')
    with open(report_json, 'w') as f:
        json.dump(report, f, indent=4, sort_keys=True)
    return successful


def _clear_fs_cache():
    execute("sudo bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")
