import getpass
import os
import time
import platform
import psutil
import json
from timeit import default_timer

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from scout.util import helper
from scout.util import aws as aws_helper

import myhadoop

@click.group()
@click.option('--hibench_dir', default="/opt/HiBench", type=click.Path(exists=True, resolve_path=True))
@click.option('--hadoop_dir', default="/opt/hadoop", type=click.Path(exists=True, resolve_path=True))
@click.option('--spark_dir', default="/opt/spark", type=click.Path(exists=True, resolve_path=True))
@click.option('--monitoring', type=click.Path(exists=False, resolve_path=True))
@click.option('--interval', type=int, default=5)
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool to benchmark a machine
    """
    ctx.obj = kwargs


@cli.command()
@click.option('--map_parallelism', type=int, default=8)
@click.option('--shuffle_parallelism', type=int, default=8)
@click.pass_context
def auto_configure(ctx, map_parallelism, shuffle_parallelism):
    configure_profiles = {
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
               am_memory=configure_profiles[instance_type][3],
               am_cores=1,
               parallelism=num_cores
               )

    # 2. configure HiBench
    # driver related
    driver_cores = 1
    driver_memory, driver_memory_ovherhead = configure_profiles[instance_type][:2]


    # executor related
    executor_cores = 2  # arbitrary
    executor_num = int(num_cores / executor_cores)
    executor_memory_overhead = configure_profiles[instance_type][2]
    executor_memory = int((memory_size - driver_memory - driver_memory_ovherhead - executor_memory_overhead * executor_num) / executor_num)

    ctx.invoke(configure,
               map_parallelism=map_parallelism,
               shuffle_parallelism=shuffle_parallelism,
               spark_master='yarn-client',
               driver_local_memory=1024,
               driver_cores=driver_cores,
               driver_memory=driver_memory,
               driver_memory_overhead=driver_memory_ovherhead,
               executor_cores=executor_cores,
               executor_memory=executor_memory,
               executor_memory_overhead=executor_memory_overhead,
               executor_num=executor_num
               )

    print("Total Cores Usage: {}/{}".format(driver_cores + executor_cores * executor_num, num_cores))
    print("Total Memory Usage: {}/{}".format(driver_memory + executor_memory * executor_num, memory_size))


@cli.command()
@click.option('--map_parallelism', type=int, default=8)
@click.option('--shuffle_parallelism', type=int, default=8)
@click.option('--spark_master', default='yarn-client')
@click.option('--driver_local_memory', type=int, default=1024, help="The driver resides in spark-submit when deploy mode is local")
@click.option('--driver_cores', type=int, default=1)
@click.option('--driver_memory', type=int, default=1024)
@click.option('--driver_memory_overhead', type=int, default=384)
@click.option('--executor_cores', type=int, default=1)
@click.option('--executor_memory', type=int, default=1024)
@click.option('--executor_memory_overhead', type=int, default=384)
@click.option('--executor_num', type=int, default=1)
@click.pass_context
def configure(ctx, map_parallelism, shuffle_parallelism, spark_master, driver_local_memory, driver_cores, driver_memory, driver_memory_overhead, executor_cores, executor_memory, executor_memory_overhead, executor_num):
    print("Map Parallelism:", map_parallelism)
    print("Shuffle Parallelism:", shuffle_parallelism)
    print("Spark Master:", spark_master)
    print("Local Driver Memory:", driver_local_memory)
    print("Driver Cores:", driver_cores)
    print("Driver Memory:", driver_memory)
    print("Driver Memory Overhead:", driver_memory_overhead)
    print("Executor Cores:", executor_cores)
    print("Executor Memory:", executor_memory)
    print("Executor Memory Overhead:", executor_memory_overhead)
    print("Number of Executor:", executor_num)
    configuration_hibench_path = os.path.join(ctx.obj['hibench_dir'], 'conf', 'hibench.conf')
    configuration_hadoop_path = os.path.join(ctx.obj['hibench_dir'], 'conf', 'hadoop.conf')
    configuration_spark_path = os.path.join(ctx.obj['hibench_dir'], 'conf', 'spark.conf')

    execute('sed -i "s/^hibench.default.map.parallelism.*/hibench.default.map.parallelism         {}/" {}'.format(map_parallelism, configuration_hibench_path))
    execute('sed -i "s/^hibench.default.shuffle.parallelism.*/hibench.default.shuffle.parallelism     {}/" {}'.format(shuffle_parallelism, configuration_hibench_path))

    #hibench.hadoop.home     /opt/hadoop
    execute('sed -i "s/^hibench.spark.master.*/hibench.spark.master    {}/" {}'.format(spark_master, configuration_spark_path))

    # executor related
    execute('sed -i "s/^hibench.yarn.executor.num.*/hibench.yarn.executor.num     {}/" {}'.format(executor_num, configuration_spark_path))
    execute('sed -i "s/^hibench.yarn.executor.cores.*/hibench.yarn.executor.cores   {}/" {}'.format(executor_cores, configuration_spark_path))
    execute('sed -i "s/^spark.executor.memory.*/spark.executor.memory {}m/" {}'.format(executor_memory, configuration_spark_path))
    execute('sed -i "s/^spark.yarn.executor.memoryOverhead.*/spark.yarn.executor.memoryOverhead {}m/" {}'.format(executor_memory_overhead, configuration_spark_path))

    # driver related
    execute('sed -i "s/^spark.driver.memory.*/spark.driver.memory {}m/" {}'.format(driver_local_memory, configuration_spark_path))
    execute('sed -i "s/^spark.yarn.am.memory .*/spark.yarn.am.memory {}m/" {}'.format(driver_memory, configuration_spark_path))
    execute('sed -i "s/^spark.yarn.am.cores.*/spark.yarn.am.cores {}/" {}'.format(driver_cores, configuration_spark_path))
    execute('sed -i "s/^spark.yarn.am.memoryOverhead.*/spark.yarn.am.memoryOverhead {}m/" {}'.format(driver_memory_overhead, configuration_spark_path))


@cli.command()
@click.option('--datasize', default='large')
@click.pass_context
def config_datasize(ctx, datasize):
    configuration_hibench_path = os.path.join(ctx.obj['hibench_dir'], 'conf', 'hibench.conf')
    execute('sed -i "s/^hibench.scale.profile.*/hibench.scale.profile                 {}/" {}'.format(datasize, configuration_hibench_path))


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
    cmd = "rm -rf {}/report/*".format(ctx.obj['hibench_dir'])
    execute(cmd)


@cli.command()
@click.option('--workload', default='wordcount')
@click.option('--datasize')
@click.pass_context
def prepare_dataset(ctx, workload, datasize):
    ctx.invoke(config_datasize, datasize=datasize)
    category = ctx.invoke(get_category, workload=workload)
    print("Preparing dataset: {} | {}".format(category, workload))
    cmd = "{}/bin/workloads/{}/{}/prepare/prepare.sh".format(ctx.obj['hibench_dir'], category, workload)
    return execute(cmd, check=False)


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
@click.option('--workload', default='wordcount')
@click.pass_context
def get_category(ctx, workload):
    workload_category_mapping = {
        'wordcount': 'micro',
        'terasort': 'micro',
        'sort': 'micro',
        'sleep': 'micro',
        'dfsioe': 'micro',
        'pagerank': 'websearch',
        'nutchindexing': 'websearch',
        'kmeans': 'ml',
        'bayes': 'ml',
        'lr': 'ml',
        'als': 'ml',
        'scan': 'sql',
        'aggregation': 'sql',
        'join': 'sql',
        'nweight': 'graph'
    }
    return workload_category_mapping[workload]

@cli.command()
@click.option('--workload', default='workdcount')
@click.option('--framework', default='spark')
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.option('--datasize')
@click.pass_context
def execute_workload(ctx, workload, framework, monitoring, interval, datasize):
    category = ctx.invoke(get_category, workload=workload)
    print("Executing workload: {} | {}".format(category, workload, framework))
    workload_dir = os.path.join(ctx.obj['hibench_dir'], 'report', workload, framework)
    execute("rm -rf {}".format(workload_dir))
    execute("mkdir -p {}".format(os.path.join(ctx.obj['hibench_dir'], 'report', workload, framework)))
    successful = False
    if monitoring:
        monitoring_output = os.path.join(ctx.obj['hibench_dir'], 'report', workload, framework, 'sar.csv')
        with HiBenchProfiler(monitoring_output, interval) as app_profiler:
            cmd = "{}/bin/workloads/{}/{}/{}/run.sh".format(ctx.obj['hibench_dir'], category, workload, framework)
            successful = execute(cmd, check=False)
    else:
        cmd = "{}/bin/workloads/{}/{}/{}/run.sh".format(ctx.obj['hibench_dir'], category, workload, framework)
        successful = execute(cmd, check=False)
    report = {
        'workload': workload,
        'framework': framework,
        'datasize': datasize,
        'completed': successful
    }
    if successful:
        result = execute("tail -n 1 {}".format(os.path.join(ctx.obj['hibench_dir'], 'report', 'hibench.report')), capture=True)
        # lazy to create variables
        report.update({
            'program': result.split()[0],
            'timestamp': " ".join(result.split()[1:3]),
            'input_size': result.split()[3],
            'elapsed_time': result.split()[4],
            'throughput_cluster': result.split()[5],
            'throughput_node': result.split()[6]
        })
    report_json = os.path.join(ctx.obj['hibench_dir'], 'report', workload, framework, 'report.json')
    with open(report_json, 'w') as f:
        json.dump(report, f, indent=4, sort_keys=True)
    return successful


@cli.command()
@click.option('-w', '--workload', help="workload.framework, e.g., wordcount.spark")
@click.option('--output_dir', default=None, type=click.Path(exists=False, resolve_path=True))
@click.option('--prepare/--no-prepare', default=False)
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.option('--datasize')
@click.pass_context
def run(ctx, workload, output_dir, prepare, monitoring, interval, datasize):
    # 2. prepare required dataset
    workload_name, framework = workload.lower().split('.')

    ctx.invoke(config_datasize, datasize=datasize)

    if prepare:
        ctx.invoke(prepare_dataset, workload=workload_name, datasize=datasize)

    # workaround to avoid failure on als, lr
    time.sleep(30)

    # 4. run and collect data
    successful = ctx.invoke(execute_workload,
               workload=workload_name,
               framework=framework,
               monitoring=monitoring,
               interval=interval,
               datasize=datasize
               )

    # 5. copy dataset to prefered place
    hibench_output_dir = os.path.join(ctx.obj['hibench_dir'], 'report', workload_name, framework)
    execute("timeout 60s bash -c 'while [ ! -f {} ]; do sleep 1; done'".format(os.path.join(hibench_output_dir, 'monitor.html')), check=False)
    execute("rm -rf {}".format(output_dir))
    execute("mkdir -p {}".format(output_dir))
    execute("cp {}/*.log {}".format(hibench_output_dir, output_dir), check=False)
    execute("cp {}/*.json {}".format(hibench_output_dir, output_dir), check=False)
    execute("cp {}/*.html {}".format(hibench_output_dir, output_dir), check=False)
    execute("cp {}/*.csv {}".format(hibench_output_dir, output_dir), check=False)
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
