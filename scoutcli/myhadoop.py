import getpass
import os
import platform
import psutil
import json
from timeit import default_timer

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from scoutcli.util import helper
from scoutcli.util import aws as aws_helper


@click.group()
@click.option('--hadoop_dir', default="/opt/hadoop", type=click.Path(exists=True, resolve_path=True))
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj = kwargs


@cli.command()
@click.pass_context
def auto_configure(ctx):
    print("Mem (GB):", aws_helper.Instance.get_memory_in_gb())
    ctx.invoke(configure,
               replicas=1,
               hostname=aws_helper.Instance.get_private_ip(),
               cores=aws_helper.Instance.get_num_of_cores(),
               memory=aws_helper.Instance.get_memory_in_gb() * 1024
    )


@cli.command()
@click.option('--replicas', type=int, default=1)
@click.option('--hostname', default=None)
@click.option('--cores', type=int, default=2)
@click.option('--memory', type=int, default=4096)
@click.option('--am_cores', type=int, default=1)
@click.option('--task_cores', type=int, default=1)
@click.option('--master')
@click.option('--slaves')
@click.pass_context
def configure(ctx, replicas, hostname, cores, memory, am_cores, task_cores, master, slaves):
    hostname = aws_helper.Instance.get_private_ip() if hostname is None else hostname
    slave_list = list(sorted(slaves.split(' ')))
    cluster_size = len(slave_list)
    cluster_cores = int(cores * cluster_size)
    cluster_memory = int(memory * cluster_size)

    memory_per_core = memory / cores
    # memory is proportional to the core counts
    am_memory = int(am_cores * memory_per_core)
    task_memory = int(task_cores * memory_per_core)

    print("* Configuring Hadoop")
    print("Hostname:", hostname)
    print("Cores per node:", cores)
    print("Memory per node:", memory)
    print("Cluster")
    print("Cluster size:", cluster_size)
    print("Master:", master)
    print("Slaves:", slave_list)
    print("Cores in Total:", cluster_cores)
    print("Memory in Total:", cluster_memory)

    print("MapReduce")
    print("Cores for AM:", am_cores)
    print("Memory for AM:", am_memory)
    print("Cores for Map/Reduce task:", task_cores)
    print("Memory for Map/Reduce task:", task_memory)

    template_hdfs = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>{}</value>
    </property>
</configuration>
'''
    template_mapreduce = '''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.cpu-vcores</name>
        <value>{}</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>{}</value>
    </property>
    <property>
        <name>mapreduce.map.cpu.vcores</name>
        <value>{}</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>{}</value>
    </property>
    <property>
        <name>mapreduce.reduce.cpu.vcores</name>
        <value>{}</value>
    </property>
    <property>
        <name>mapreduce.map.maxattempts</name>
        <value>1</value>
    </property>
    <property>
        <name>mapreduce.reduce.maxattempts</name>
        <value>1</value>
    </property>
</configuration>
'''
    template_yarn = '''<?xml version="1.0"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>128</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-vcores</name>
        <value>1</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>{}</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.am.max-attempts</name>
        <value>1</value>
    </property>
</configuration>
'''
    template_core = '''<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://{}:9000</value>
    </property>
</configuration>
'''

    template_hdfs_content = template_hdfs.format(replicas)
    template_mapreduce_content = template_mapreduce.format(am_memory, am_cores, task_memory, task_cores, task_memory, task_cores)
    template_yarn_content = template_yarn.format(master, memory, cores, memory, cores) # one extra core for AM in Yarn
    template_core_content = template_core.format(master)
    configuration_hdfs_path = os.path.join(ctx.obj['hadoop_dir'], 'etc', 'hadoop', 'hdfs-site.xml')
    configuration_mapreduce_path = os.path.join(ctx.obj['hadoop_dir'], 'etc', 'hadoop', 'mapred-site.xml')
    configuration_yarn_path = os.path.join(ctx.obj['hadoop_dir'], 'etc', 'hadoop', 'yarn-site.xml')
    configuration_core_path = os.path.join(ctx.obj['hadoop_dir'], 'etc', 'hadoop', 'core-site.xml')
    configuration_slaves_path = os.path.join(ctx.obj['hadoop_dir'], 'etc', 'hadoop', 'slaves')
    with open(configuration_hdfs_path, 'w') as f:
        f.write(template_hdfs_content)
    with open(configuration_mapreduce_path, 'w') as f:
        f.write(template_mapreduce_content)
    with open(configuration_yarn_path, 'w') as f:
        f.write(template_yarn_content)
    with open(configuration_core_path, 'w') as f:
        f.write(template_core_content)
    with open(configuration_slaves_path, 'w') as f:
        for slave in slave_list:
            f.write(slave + "\n")
    

@cli.command()
@click.pass_context
def init(ctx):
    # workaround to remove storage dir for the datanode
    execute("rm -rf /tmp/hadoop-*")
    execute("{}/bin/hdfs namenode -format -force -nonInteractive".format(ctx.obj['hadoop_dir']))

@cli.command()
@click.pass_context
def start(ctx):
    # 1. Start HDFS
    execute("{}/sbin/start-dfs.sh".format(ctx.obj['hadoop_dir']))
    # 2. Start Yarn
    execute("{}/sbin/start-yarn.sh".format(ctx.obj['hadoop_dir']))


@cli.command()
@click.pass_context
def stop(ctx):
    # 1. Stop Yarn
    execute("{}/sbin/stop-yarn.sh".format(ctx.obj['hadoop_dir']))
    # 2. Stop HDFS
    execute("{}/sbin/stop-dfs.sh".format(ctx.obj['hadoop_dir']))
