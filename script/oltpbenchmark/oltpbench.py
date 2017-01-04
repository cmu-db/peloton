#!/usr/bin/env python

import sys
import os
import subprocess

OLTP_HOME = "/home/pavlo/Documents/OLTPBenchmark/oltpbench"
BENCHMARK = "tpcc"
CONFIG = "%s/config/%s_config_peloton.xml" % (OLTP_HOME, BENCHMARK)
INTERVAL = 1
BUCKETS = 5

def execute(create=True, load=True, execute=False):
    params = {
        "home": OLTP_HOME,
        "benchmark": BENCHMARK,
        "config": CONFIG,
        "create": str(create),
        "load": str(load),
        "execute": str(execute),
        "interval": INTERVAL,
        "buckets": BUCKETS,
    }
    cmd = "%(home)s/oltpbenchmark " \
          "-b %(benchmark)s " \
          "-c %(config)s " \
          "--create=%(create)s " \
          "--load=%(load)s " \
          "--execute=%(execute)s " \
          "--histograms " \
          "-im %(interval)d " \
          "-s %(buckets)d" % params
    
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line

    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
## DEF