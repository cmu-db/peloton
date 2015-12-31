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

def install_dependencies(TEMPDIR):

    ## ==============================================
    ## NVM Library
    ## ==============================================
    LOG.info("Cloning NVM library")
    cmd = 'git clone https://github.com/pmem/nvml'
    exec_cmd(cmd)

    LOG.info("Installing NVM library")
    os.chdir('nvml')
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing NVM library")

    ## ==============================================
    ## Nanomsg Library
    ## ==============================================
    LOG.info("Downloading nanomsg library")
    cmd = 'wget https://github.com/nanomsg/nanomsg/releases/download/0.8-beta/nanomsg-0.8-beta.tar.gz'
    exec_cmd(cmd)
    cmd = 'tar -xzf nanomsg-0.8-beta.tar.gz'
    exec_cmd(cmd)

    LOG.info("Installing NVM library")
    os.chdir('nanomsg-0.8-beta')
    cmd = './configure'
    exec_cmd(cmd)
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing nanomsg library")

## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Install Dependencies')

    args = parser.parse_args()

    try:
        # Set up tmp dir
        TEMPDIR = tempfile.mkdtemp()
        LOG.info("Building and installing dependencies...")
        LOG.info("Temporary directory : " + str(TEMPDIR))

        prev_dir = os.getcwd()
        os.chdir(TEMPDIR)

        install_dependencies(TEMPDIR)

    finally:
        # Clean up
        shutil.rmtree(TEMPDIR)

        # Go back to prev dir
        os.chdir(prev_dir)

