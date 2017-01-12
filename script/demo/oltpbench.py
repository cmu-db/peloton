#!/usr/bin/env python

import sys
import os
import re
import subprocess

OLTP_HOME = "/home/pavlo/Documents/OLTPBenchmark/oltpbench"
BENCHMARK = "tpcc"
CONFIG = "%s/config/%s_config_peloton.xml" % (OLTP_HOME, BENCHMARK)
BUCKETS = 5

REGEX = re.compile(".*?INFO[\s]+-[\s]+Throughput: ([\d]+\.[\d]+) txn/sec")

def executeBenchmark(create=True, load=True, execute=False, interval=1000):
    params = {
        "home": OLTP_HOME,
        "benchmark": BENCHMARK,
        "config": CONFIG,
        "create": str(create),
        "load": str(load),
        "execute": str(execute),
        "interval": interval,
        "buckets": BUCKETS,
    }
    cmd = "cd %(home)s && %(home)s/oltpbenchmark " \
          "-b %(benchmark)s " \
          "-c %(config)s " \
          "--create=%(create)s " \
          "--load=%(load)s " \
          "--execute=%(execute)s " \
          "--histograms " \
          "-im %(interval)d " \
          "-s %(buckets)d " % params
    
    ssh_cmd = [
        "ssh", "ottertune", cmd 
    ]
    
    os.chdir(OLTP_HOME)
    popen = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for line in iter(popen.stdout.readline, ""):
        m = REGEX.search(line)
        if m:
            yield float(m.groups()[0])
             #yield stdout_line
    ## FOR

    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
## DEF

def pollFile(path):
    cmd = [ "tail", "-F", "-n", "0", path ]
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for line in iter(popen.stdout.readline, ""):
        yield line.strip()
    ## FOR
## DEF
