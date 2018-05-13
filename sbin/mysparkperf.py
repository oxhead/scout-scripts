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

import myhadoop

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
@click.pass_context
def auto_configure(ctx):
    # step
    # 1. configure Hadoop, HDFS and Yarn
    hostname = aws_helper.Instance.get_private_ip()
    instance_type = aws_helper.Instance.get_instance_type()
    print("Instance Type:", instance_type)
    num_cores = aws_helper.Instance.get_num_of_cores()
    memory_size = ctx.invoke(get_memory, instance=instance_type)
    ctx.invoke(myhadoop.configure,
               replicas=1,
               hostname=hostname,
               cores=num_cores,
               memory=memory_size,
               am_memory=ctx.invoke(get_config_profile, instance=instance_type)[3],
               am_cores=1,
               parallelism=num_cores
               )

    # 2. configure HiBench
    # driver related
    driver_cores = 1
    driver_memory, driver_memory_ovherhead = ctx.invoke(get_config_profile, instance=instance_type)[:2]

    # executor related
    executor_cores = 2  # arbitrary
    executor_num = int(num_cores / executor_cores)
    executor_memory_overhead = ctx.invoke(get_config_profile, instance=instance_type)[2]
    executor_memory = int((memory_size - driver_memory - driver_memory_ovherhead - executor_memory_overhead * executor_num) / executor_num)

    print("Total Cores Usage: {}/{}".format(driver_cores + executor_cores * executor_num, num_cores))
    print("Total Memory Usage: {}/{}".format(driver_memory + executor_memory * executor_num, memory_size))


@cli.command()
@click.option('--instance', default='c4.large')
@click.pass_context
def get_spark_env(ctx, instance):
    num_cores = aws_helper.Instance.get_num_of_cores()
    memory_size = ctx.invoke(get_memory, instance=instance)

    driver_cores = 1
    driver_memory, driver_memory_overhead = ctx.invoke(get_config_profile, instance=instance)[:2]

    executor_cores = 2  # arbitrary
    executor_num = int(num_cores / executor_cores)
    executor_memory_overhead = ctx.invoke(get_config_profile, instance=instance)[2]
    executor_memory = int((memory_size - driver_memory - driver_memory_overhead - executor_memory_overhead * executor_num) / executor_num)

    env_settings = {
        #'spark.driver.memory': '{}m'.format(driver_memory),
        'spark.executor.instances': executor_num,
        'spark.driver.memory': '{}m'.format(1024),
        'spark.yarn.driver.memoryOverhead': driver_memory_overhead,
        'spark.executor.memory': '{}m'.format(executor_memory),
        'spark.yarn.executor.memoryOverhead': executor_memory_overhead,  # no unit, different from Spark 2.1
        'spark.yarn.am.cores': driver_cores,
        'spark.yarn.am.memory': '{}m'.format(driver_memory),
        'spark.yarn.am.memoryOverhead': driver_memory_overhead,  # no unit, different from Spark 2.1
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
@click.option('--instance', default='c4.large')
@click.option('-w', '--workload', help="workload.framework, e.g., wordcount.spark")
@click.option('--datasize', default='size1')
@click.option('--output_dir', default=None, type=click.Path(exists=False, resolve_path=True))
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.pass_context
def run(ctx, instance, workload, datasize, output_dir, monitoring, interval):
    execute("rm -rf {}; mkdir -p {}".format(output_dir, output_dir))

    # 2. prepare env setting?
    # @TODO: num-partitions?  x cores?
    spark_env = ctx.invoke(get_spark_env, instance=instance)
    env_settings = {
        'HADOOP_CONF_DIR': "{}/etc/hadoop".format(ctx.obj['hadoop_dir']),
        'SPARK_SUBMIT_OPTS': " ".join(["-D{}={}".format(k, spark_env[k]) for k in spark_env.keys()])
    }

    execute("export | grep HADOOP_CONF_DIR", environment=env_settings)
    execute("export | grep SPARK_SUBMIT_OPTS", environment=env_settings)

    # 1. generate commands
    cmd = ctx.invoke(generate_command, workload=workload, datasize=datasize, num_partitions=spark_env['sparkperf.executor.num'], output_dir=output_dir)
    print(cmd)

    timestampt = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    if monitoring:
        monitoring_output = os.path.join(output_dir, 'sar.csv')
        with HiBenchProfiler(monitoring_output, interval) as app_profiler:
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
    }
    if successful:
        # lazy to create variables
        report.update({
            'program': workload,
            'timestamp': timestampt,
            'input_size': -1,
            'elapsed_time': timer.elapsed_secs,
            'throughput_cluster': -1,
            'throughput_node': -1
        })

    report_json = os.path.join(output_dir, 'report.json')
    with open(report_json, 'w') as f:
        json.dump(report, f, indent=4, sort_keys=True)
    return successful


def _clear_fs_cache():
    execute("sudo bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")


class HiBenchProfiler(object):
    def __init__(self, monitoring_output, monitoring_interval=5, verbose=False):
        self.verbose = verbose
        self.timer = default_timer
        self.monitoring_output = monitoring_output
        self.monitoring_data = monitoring_output + ".dat"
        self.monitoring_interval = monitoring_interval
        print("Monitoring output:", self.monitoring_output)
        print("Monitoring data:", self.monitoring_data)
        print("Monitoring interval:", self.monitoring_interval)

    def __enter__(self):
        print("mysar start --output={} --interval={}".format(self.monitoring_data, self.monitoring_interval))
        execute("mysar start --output={} --interval={}".format(self.monitoring_data, self.monitoring_interval))
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        self.end = self.timer()
        self.elapsed_secs = self.end - self.start
        self.elapsed = self.elapsed_secs * 1000  # millisecs
        execute("mysar stop")
        execute("mysar export --input={} --output={} --interval={}".format(self.monitoring_data, self.monitoring_output, self.monitoring_interval))
        if self.verbose:
            print('elapsed time: %f ms' % self.elapsed)
