import getpass
import os
import time
import platform
import json
import datetime
from timeit import default_timer

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from scoutcli.utils import helper
from scoutcli.utils import aws as aws_helper
from scoutcli.utils import parallel


@click.group()
@click.option('--hpcc_dir', default="/opt/HPCCSystems", type=click.Path(exists=True, resolve_path=True))
@click.option('--hpcc_conf_dir', default="/etc/HPCCSystems", type=click.Path(exists=True, resolve_path=True))
@click.option('--benchmark_dir', default="/opt/hpcc_benchmark", type=click.Path(exists=True, resolve_path=True))
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
    slave_list = slaves.split()
    parallelism = aws_helper.Instance.get_num_of_cores() // 2 
    ctx.invoke(configure,
               master=master,
               slaves=slaves,
               parallelism=parallelism,
               )


@cli.command()
@click.option('--master')
@click.option('--slaves')
@click.option('--parallelism', type=int, default=1)
@click.pass_context
def configure(ctx, master, slaves, parallelism):
    print("* Configuring HPCC")
    print("Parallelism:", parallelism)
    configuration_hpcc_path = os.path.join(ctx.obj['hpcc_conf_dir'], 'environment.xml')
    host_file = '/tmp/ipfile'
    host_list = slaves.split()
    with open(host_file, 'w') as f:
        for host in host_list:
            f.write(host + '\n')
    num_thors = len(host_list)
    
    # disable roxie for now
    # use the virtual thor mode
    execute('{}/sbin/envgen -env {} -ipfile {} -supportnodes 0 -thornodes {} -roxienodes 0 -roxieondemand 0 -slavesPerNode 1 -set_xpath_attrib_value Software/ThorCluster @channelsPerSlave {}'.format(ctx.obj['hpcc_dir'], configuration_hpcc_path, host_file, num_thors, parallelism))


@cli.command()
@click.pass_context
def start(ctx):
    execute('sudo service hpcc-init start')


@cli.command()
@click.pass_context
def stop(ctx:
    execute('sudo service hpcc-init stop; sudo service dafilesrv stop')


@cli.command()
@click.pass_context
def init(ctx):
    ctx.invoke(stop)
    ctx.invoke(clean)


@cli.command()
@click.pass_context
def clean(ctx):
    execute('sudo rm -rf /var/log/HPCCSystems/; sudo rm -rf /var/lib/HPCCSystems/')

def _get_datasize_parameters(workload_name, datasize):
    parameters = {
        ('rf-classification', 'small'): {
            'num_records_training': 10000,
            'num_records_test': 10000,
            'num_trees': 20,
        },
        ('rf-classification', 'medium'): {
            'num_records_training': 20000,
            'num_records_test': 20000,
            'num_trees': 20,
        },
        ('rf-classification', 'large'): {
            'num_records_training': 50000,
            'num_records_test': 50000,
            'num_trees': 20,
        },
        ('rf-regression', 'small'): {
            'num_records_training': 10000,
            'num_records_test': 10000,
            'num_trees': 20,
        },
        ('rf-regression', 'medium'): {
            'num_records_training': 20000,
            'num_records_test': 20000,
            'num_trees': 20,
        },
        ('rf-regression', 'large'): {
            'num_records_training': 50000,
            'num_records_test': 50000,
            'num_trees': 20,
        },
        ('lr', 'small'): {
            'num_workers': 2,
            'item_size': 10000,
            'num_columns': 100,
            'num_iterations': 10,
        },
        ('lr', 'medium'): {
            'num_workers': 4,
            'item_size': 20000,
            'num_columns': 100,
            'num_iterations': 50,
        },
        ('lr', 'small'): {
            'num_workers': 8,
            'item_size': 50000,
            'num_columns': 100,
            'num_iterations': 100,
        },
        ('glm', 'small'): {
            'num_workers': 2,
            'item_size': 10000,
            'num_columns': 100,
            'num_iterations': 10,
        },
        ('glm', 'medium'): {
            'num_workers': 4,
            'item_size': 20000,
            'num_columns': 100,
            'num_iterations': 50,
        },
        ('glm', 'small'): {
            'num_workers': 8,
            'item_size': 50000,
            'num_columns': 100,
            'num_iterations': 100,
        },
    }
    return parameters[(workload_name, datasize)]


@cli.command()
@click.option('--workload', default='lr')
@click.option('--framework', default='spark')
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.option('--timeout', type=int)
@click.option('--datasize')
@click.option('--slaves')
@click.option('--output_dir')
@click.pass_context
def execute_workload(ctx, workload, framework, monitoring, interval, timeout, datasize, slaves, output_dir):
    print("Executing workload: {} on {}".format(workload, framework))
    ecl_program = os.path.join(ctx.obj['benchmark_dir'], "{}.ecl".format(workload))
    
    execute("rm -rf {}".format(output_dir))
    execute("mkdir -p {}".format(output_dir))

    successful = False
    parameters = "".join([' -X{}={} '.format(k, v) for k, v in _get_datasize_parameters(workload, datasize).items()])
    cmd = "set -o pipefail; timeout {}s {}/bin/ecl run --target {} --ecl-only {} {} |& tee {}/result.log".format(timeout, ctx.obj['hpcc_dir'], framework, parameters, ecl_program, output_dir)
    timestampt = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    if monitoring:
        monitoring_output = os.path.join(output_dir, 'sar.csv')
        slave_list = list(sorted(slaves.split(' ')))
        with ClusterProfiler(slave_list, monitoring_output, interval) as app_profiler:
            with helper.Timer() as timer:
                successful = execute(cmd, check=False)
    else:
        with helper.Timer() as timer:
            successful = execute(cmd, check=False)
    report = {
        'workload': workload,
        'framework': framework,
        'datasize': datasize,
        'input_size': '-1',
        'completed': successful
    }

    report = {
        'workload': workload,
        'framework': framework,
        'datasize': datasize,
        'completed': successful,
        'program': ecl_program,
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


@cli.command()
@click.option('-w', '--workload', help="workload name")
@click.option('--output_dir', default=None, type=click.Path(exists=False, resolve_path=True))
@click.option('--monitoring/--no-monitoring', default=True)
@click.option('--interval', type=int, default=5)
@click.option('--timeout', type=int, default=60*60*24)
@click.option('--datasize')
@click.option('--slaves')
@click.option('--mode')
@click.pass_context
def run(ctx, workload, output_dir, monitoring, interval, timeout, datasize, slaves, mode):
    workload_name = workload.lower()
    framework = 'thor'

    _clear_fs_cache()

    # run and collect data
    successful = ctx.invoke(execute_workload,
               workload=workload_name,
               framework='thor',
               monitoring=monitoring,
               interval=interval,
               timeout=timeout,
               datasize=datasize,
               slaves=slaves,
               output_dir=output_dir
               )
    return successful


def _clear_fs_cache():
    execute("sudo bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")


class ClusterProfiler(object):
    def __init__(self, nodes, monitoring_output, monitoring_interval=5, verbose=False):
        self.verbose = verbose
        self.timer = default_timer
        self.nodes = nodes
        self.monitoring_output = monitoring_output
        self.monitoring_data = monitoring_output + ".dat"
        self.monitoring_interval = monitoring_interval
        print("Monitoring output:", self.monitoring_output)
        print("Monitoring data:", self.monitoring_data)
        print("Monitoring interval:", self.monitoring_interval)
        self.output_records = {self.nodes[i]: self.monitoring_output.replace('sar.csv', 'sar_node{}.csv'.format(i+1)) for i in range(len(self.nodes))}
        print(self.output_records)

    def __enter__(self):
        cmd = "mysar start --output={} --interval={}".format(self.monitoring_data, self.monitoring_interval)
        print(cmd)
        with parallel.CommandAgent(show_result=False, concurrency=len(self.nodes)) as agent:
            # the long timeout avoid irresponsible nodes due to heavy loading
            agent.submit_remote_commands(
                self.nodes,
                cmd,
                connect_timeout=60,
                silent=True)
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        self.end = self.timer()
        self.elapsed_secs = self.end - self.start
        self.elapsed = self.elapsed_secs * 1000  # millisecs

        with parallel.CommandAgent(show_result=False, concurrency=len(self.nodes)) as agent:
            for node in self.nodes:
                cmd = "mysar stop; mysar export --input={} --output={} --interval={}".format(self.monitoring_data, self.output_records[node], self.monitoring_interval)
                print(cmd)
                agent.submit_remote_command(
               	    node,
                    cmd,
                    connect_timeout=60,
                    silent=True)

        if self.verbose:
            print('elapsed time: %f ms' % self.elapsed)
