import getpass
import os
import platform
import json

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from scoutcli.utils import parallel

@click.group()
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool to benchmark a machine
    """
    ctx.obj = kwargs


@cli.command()
@click.pass_context
@click.option('--output', default="/tmp/sar.dat", type=click.Path(exists=False, resolve_path=True))
@click.option('--interval', default=5, type=int)
def start(ctx, output, interval):
    execute("rm -f".format(output))
    execute("mkdir -p {}".format(os.path.dirname(output)))
    cmd_start = "nohup sar -p -A -o {} {} > /dev/null 2>&1 &".format(output, interval)
    # print(cmd_start)
    execute(cmd_start)

@cli.command()
@click.pass_context
def stop(ctx):
    cmd_kill = "pkill -x sar"
    execute(cmd_kill)


@cli.command()
@click.option('--input', default="/tmp/sar.dat", type=click.Path(exists=True, resolve_path=True))
@click.option('--output', default="/tmp/sar.csv", type=click.Path(exists=False, resolve_path=True))
@click.option('--interval', default=5, type=int)
@click.pass_context
def export(ctx, input, output, interval):
    execute('mkdir -p {}'.format(os.path.dirname(output)))
    # TODO: this tool does not support multiple devices
    cmd_general = 'sadf -dh -t {} {} -- -p -bBqSwW -u ALL -I SUM -r ALL | csvcut -d ";" -C "# hostname,interval,CPU,INTR" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_general.csv")
    cmd_disk = 'sadf -dh -t {} {} -- -p -d | csvcut -d ";" -C "# hostname,interval,DEV" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_disk.csv")
    cmd_network = 'sadf -dh -t {} {} -- -p -n DEV | csvcut -d ";" -C "# hostname,interval,IFACE" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_network.csv")
    cmd_join = 'csvjoin -I -c timestamp /tmp/sar_general.csv /tmp/sar_disk.csv | csvjoin -I -c timestamp - /tmp/sar_network.csv | csvformat -K 1 > {}'.format("/tmp/sar_join.csv")
    modified_header = 'timestamp,cpu.%usr,cpu.%nice,cpu.%sys,cpu.%iowait,cpu.%steal,cpu.%irq,cpu.%soft,cpu.%guest,cpu.%gnice,cpu.%idle,task.proc/s,task.cswch/s,intr.intr/s,swap.pswpin/s,swap.pswpout/s,paging.pgpgin/s,paging.pgpgout/s,paging.fault/s,paging.majflt/s,paging.pgfree/s,paging.pgscank/s,paging.pgscand/s,paging.pgsteal/s,paging.%vmeff,io.tps,io.rtps,io.wtps,io.bread/s,io.bwrtn/s,memory.kbmemfree,memory.kbavail,memory.kbmemused,memory.%memused,memory.kbbuffers,memory.kbcached,memory.kbcommit,memory.%commit,memory.kbactive,memory.kbinact,memory.kbdirty,memory.kbanonpg,memory.kbslab,memory.kbkstack,memory.kbpgtbl,memory.kbvmused,swap.kbswpfree,swap.kbswpused,swap.%swpused,swap.kbswpcad,swap.%swpcad,load.runq-sz,load.plist-sz,load.ldavg-1,load.ldavg-5,load.ldavg-15,load.blocked,disk.tps,disk.rd_sec/s,disk.wr_sec/s,disk.avgrq-sz,disk.avgqu-sz,disk.await,disk.svctm,disk.%util,network.rxpck/s,network.txpck/s,network.rxkB/s,network.txkB/s,network.rxcmp/s,network.txcmp/s,network.rxmcst/s,network.%ifutil'
    cmd_create = 'echo {} > {}'.format(modified_header, output)
    cmd_header = 'cat {} >> {}'.format("/tmp/sar_join.csv", output)
    execute(cmd_general)
    execute(cmd_disk)
    execute(cmd_network)
    execute(cmd_join)
    execute(cmd_create)
    execute(cmd_header)
