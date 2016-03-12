#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Install Dependencies
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
import tempfile

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

my_env = os.environ.copy()
FILE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.join(os.path.dirname(FILE_DIR), os.pardir)
THIRD_PARTY_DIR = os.path.join(ROOT_DIR, "third_party")
SRC_DIR = os.path.join(ROOT_DIR, "src")

## ==============================================
## Utilities
## ==============================================

def exec_cmd(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)
    verbose = True

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

def install_dependencies():

    LOG.info(os.getcwd())
    LOG.info(FILE_DIR)
    LOG.info(ROOT_DIR)
    
## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Install Dependencies')

    args = parser.parse_args()

    try:
        prev_dir = os.getcwd()
        install_dependencies()

    finally:
        # Go back to prev dir
        os.chdir(prev_dir)

