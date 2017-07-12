#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import subprocess
import time
import re
import platform

## ==============================================
## CONFIGURATION
## ==============================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/validators/<this_file>
# PELOTON_DIR needs to be redefined if the directory structure is changed
CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
PELOTON_DIR = reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])

# Change if Peloton bin directory is modified
PELOTON_BIN = os.path.join(PELOTON_DIR, "build/bin/peloton")

# Path to JDBC test script
PELOTON_JDBC_SCRIPT_DIR = os.path.join(PELOTON_DIR, "script/testing/jdbc")

PELOTON_PORT = 15721

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

def runTest(script, enableStats=False, isOSX=False):
    # Launch Peloton
    args = "-port " + str(PELOTON_PORT)
    if enableStats:
        args += " -stats_mode 1"
    if isOSX:
        cmd = ['ASAN_OPTIONS=detect_container_overflow=0 exec ' + PELOTON_BIN + ' ' + args]
    else:
        cmd = ['exec ' + PELOTON_BIN + ' ' + args]
    peloton = subprocess.Popen(cmd, shell=True)
    time.sleep(3) # HACK

    # start jdbc test
    os.chdir(PELOTON_JDBC_SCRIPT_DIR)
    jdbc = subprocess.Popen(['/bin/bash ' + script + ' basic'], shell=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    jdbc_out = ''.join(jdbc.stdout.readlines())
    print jdbc_out

    # kill peloton
    peloton.kill()

    # any mention of the word 'exception'? Fail validator
    if re.search('exception', jdbc_out, re.IGNORECASE):
        sys.exit(EXIT_FAILURE)
## DEF


if __name__ == '__main__':
    if not os.path.exists(PELOTON_BIN):
        raise Exception("Unable to find Peloton binary '%s'" % PELOTON_BIN)
    if not os.path.exists(PELOTON_JDBC_SCRIPT_DIR):
        raise Exception("Unable to find JDBC script dir '%s'" % PELOTON_JDBC_SCRIPT_DIR)

    ## Basic
    runTest("test_jdbc.sh", isOSX=platform.system() == 'Darwin')

    ## Stats
    # runTest("test_jdbc.sh", enableStats=True, platform.system() == 'Darwin')

## MAIN
