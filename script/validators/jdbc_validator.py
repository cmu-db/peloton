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

# JDBC Test Classses
# These are in PELOTON_JDBC_SCRIPT_DIR
# Yes, I know that this is ghetto
PELOTON_JDBC_TESTS = [
    "PelotonBasicTest",
    "PelotonTypeTest",
]

PELOTON_DB_HOST = "localhost"
PELOTON_DB_PORT = 15721

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

def runTest(script, target, enableStats=False, isOSX=False):
    # Launch Peloton
    peloton_args = "-port " + str(PELOTON_DB_PORT)
    if enableStats:
        peloton_args += " -stats_mode 1"
    if isOSX:
        peloton_cmd = ['ASAN_OPTIONS=detect_container_overflow=0 exec ' + PELOTON_BIN + ' ' + peloton_args]
    else:
        peloton_cmd = ['exec ' + PELOTON_BIN + ' ' + peloton_args]
    peloton = subprocess.Popen(peloton_cmd, shell=True)
    time.sleep(3) # HACK

    # start jdbc test
    jdbc_cmd = [ " ".join(['/bin/bash', script, target, PELOTON_DB_HOST, str(PELOTON_DB_PORT)]) ]
    os.chdir(PELOTON_JDBC_SCRIPT_DIR)    
    jdbc = subprocess.Popen(jdbc_cmd, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    jdbc_out = ''.join(jdbc.stdout.readlines())
    print jdbc_out.strip()

    # kill peloton
    peloton.kill()

    # any mention of the word 'exception'? Fail validator
    if re.search('exception', jdbc_out, re.IGNORECASE):
        print "FAIL: %s" % target
        sys.exit(EXIT_FAILURE)
        
    print "SUCCESS: %s" % target
## DEF


if __name__ == '__main__':
    if not os.path.exists(PELOTON_BIN):
        raise Exception("Unable to find Peloton binary '%s'" % PELOTON_BIN)
    if not os.path.exists(PELOTON_JDBC_SCRIPT_DIR):
        raise Exception("Unable to find JDBC script dir '%s'" % PELOTON_JDBC_SCRIPT_DIR)

    isOSX = platform.system() == 'Darwin'

    first = True
    for target in PELOTON_JDBC_TESTS:
        if first:
            first = False
        else:
            print "="*100
        runTest("test_jdbc.sh", target, isOSX=isOSX)

    ## Stats
    # runTest("test_jdbc.sh", enableStats=True, platform.system() == 'Darwin')

## MAIN
