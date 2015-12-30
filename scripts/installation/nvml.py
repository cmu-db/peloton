#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Install NVM library
## ==============================================

import sys
import shlex
import shutil
import tempfile
import os
import time
import logging
import argparse
import pprint
import numpy
import re
import fnmatch
import string
import subprocess

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)


## ==============================================
## CONFIGURATION
## ==============================================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT_DIR = reduce(os.path.join, [BASE_DIR, os.path.pardir, os.path.pardir])
NVML_DIR = reduce(os.path.join, [ROOT_DIR, 'third_party', 'nvml'])

my_env = os.environ.copy()

## ==============================================
## Utilities
## ==============================================

def exec_cmd(cmd, verbose):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)

    # TRY
    FNULL = open(os.devnull, 'w')
    try:
        if verbose == True:
            subprocess.check_call(args, env=my_env)
        else:
            subprocess.check_call(args, stdout=FNULL, stderr=subprocess.STDOUT, env=my_env)
    # Exception
    except subprocess.CalledProcessError as e:
        print "Command     :: ", e.cmd
        print "Return Code :: ", e.returncode
        print "Output      :: ", e.output            
    # Finally
    finally:
        FNULL.close()

def build_library():
    prev_dir = os.getcwd()
    
    LOG.info("Building NVM library")
    os.chdir(NVML_DIR)

    cmd = 'make -j4'
    exec_cmd(cmd, False)

    LOG.info("Finished building NVM library")
    
    os.chdir(prev_dir)

def install_library():
    prev_dir = os.getcwd()
    
    os.chdir(NVML_DIR)

    LOG.info("Installing NVM library")

    cmd = 'make install -j4'
    exec_cmd(cmd, False)

    LOG.info("Finished installing NVM library")
    
    os.chdir(prev_dir)
    
## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Build and Install NVM Library')

    parser.add_argument("-b", "--build", help='build library', action='store_true')
    parser.add_argument("-i", "--install", help='install library', action='store_true')

    args = parser.parse_args()

    if args.build:
        build_library()

    if args.install:
        install_library()
