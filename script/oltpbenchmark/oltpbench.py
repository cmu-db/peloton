#!/usr/bin/env python

import sys
import os
import re
import subprocess

OLTP_HOME = "/home/pavlo/Documents/OLTPBenchmark/oltpbench"
BENCHMARK = "tpcc"
CONFIG = "%s/config/%s_config_peloton.xml" % (OLTP_HOME, BENCHMARK)
INTERVAL = 1000
BUCKETS = 5

REGEX = re.compile(".*?INFO[\s]+-[\s]+Throughput: ([\d]+\.[\d]+) txn/sec")

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
    cmd = [
        "%(home)s/oltpbenchmark" % params,
        "-b %(benchmark)s" % params,
        "-c %(config)s" % params,
        "--create=%(create)s" % params,
        "--load=%(load)s" % params,
        "--execute=%(execute)s" % params,
        "--histograms" % params,
        "-im %(interval)d" % params,
        "-s %(buckets)d" % params,
    ]
    
    os.chdir(OLTP_HOME)
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
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