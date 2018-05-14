import math

from executor import execute


class Instance:
    def get_private_ip():
        # not sure what will happen to the case with multiple network interfaces
        return execute('curl http://169.254.169.254/latest/meta-data/local-ipv4', capture=True, silent=True)

    def get_public_ip():
        return execute('curl http://169.254.169.254/latest/meta-data/public-ipv4', capture=True, silent=True)

    def get_instance_type():
        return execute('curl http://169.254.169.254/latest/meta-data/instance-type', capture=True, silent=True)

    def get_instance_id():
        return execute('curl http://169.254.169.254/latest/meta-data/instance-id', capture=True, silent=True)

    def get_num_of_cores():
        return int(execute('getconf _NPROCESSORS_ONLN', capture=True, silent=True))

    def get_memory_in_gb():
        mem_gb = int(math.ceil(float(execute('cat /proc/meminfo  | grep -i memtotal', capture=True, silent=True).split(' ')[-2]) / 1048576))
        return mem_gb
