import os
import time
from timeit import default_timer
import datetime
import uuid
import math
import hashlib
import json
import re


def get_timestamp():
    now = datetime.datetime.now()
    time_string = now.strftime("%Y%m%d%H%M%S")
    return time_string


def get_tmp_file():
    return "/tmp/%s" % str(uuid.uuid4())


def find_nearest_power_2(n):
    return pow(2, int(math.log(n, 2) + 0.5))


def string_grep(s, word):
    for line in s.split("\n"):
        if word in line:
            return line
    return None


def parse_file(file_path):
    with open(file_path, 'r') as f:
        return [row.rstrip() for row in f.read().splitlines()]


def md5hash_raw(s):
    return hashlib.md5(s.encode('utf-8'))


def md5hash(content):
    if type(content) is str:
        return int(md5hash_raw(content).hexdigest(), 16)
    else:
        m = hashlib.md5()
        for s in content:
            m.update(md5hash_raw(repr(s)).digest())
        return int(m.hexdigest(), 16)

def json_pretty_print(json_object):
    print(json.dumps(json_object, indent=4, sort_keys=True))


def simple_string_match(pattern, target):
    #print(pattern, target)
    return len(re.findall(pattern, target)) > 0


class Timer(object):
    '''
    http://coreygoldberg.blogspot.com/2012/06/python-timer-class-context-manager-for.html
    '''
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.timer = default_timer

    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        self.end = self.timer()
        self.elapsed_secs = self.end - self.start
        self.elapsed = self.elapsed_secs * 1000  # millisecs
        if self.verbose:
            print
            'elapsed time: %f ms' % self.elapsed
