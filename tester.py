#!/usr/bin/env python
# Feature generator

import os
import subprocess
import logging
import time
import argparse

from subprocess import CalledProcessError

# # LOGGING CONFIGURATION
LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

# # CONFIGURATION
BASE_DIR = os.path.dirname(__file__)

MAKE = "make"
PG_CTL = "pg_ctl"
PSQL = "psql"

PG_DATA_DIR = BASE_DIR + "/data"
LOG_FILE_NAME = BASE_DIR + "/data/log/setup.log"


# PG
def execute_pg_build(verbose_mode):
    LOG.info("Building PG")

    try:
        log_file = open(LOG_FILE_NAME, 'w')

        if verbose_mode:
            subprocess.check_call([MAKE], stdout=log_file)
            subprocess.check_call([MAKE, 'install'], stdout=log_file)
        else:
            subprocess.check_call([MAKE, '--silent'], stdout=log_file)
            subprocess.check_call([MAKE, 'install', '--silent'],
                                  stdout=log_file)

        log_file.close()

    except CalledProcessError as e:
        print(e.returncode)


# PG_CTL
def execute_pg_ctl(pg_ctl_mode):

    try:
        log_file = open(LOG_FILE_NAME, 'w')

        if pg_ctl_mode == 1 or pg_ctl_mode == 3:
            # Stop PG
            LOG.info("Stopping PG")
            subprocess.check_call([PG_CTL,
                                   '-D', PG_DATA_DIR,
                                   '-l', LOG_FILE_NAME, 'stop'],
                                  stdout=log_file)

        if pg_ctl_mode == 1 or pg_ctl_mode == 2:
            # Start PG
            LOG.info("Starting PG")
            subprocess.check_call([PG_CTL,
                                   '-D', PG_DATA_DIR,
                                   '-l', LOG_FILE_NAME, 'start'],
                                  stdout=log_file)

        log_file.close()

    except CalledProcessError as e:
        print(e.returncode)


# PSQL
def execute_psql():
    LOG.info("Starting PSQL")

    try:
        log_file = open(LOG_FILE_NAME, 'w')
        time.sleep(5)
        subprocess.check_call([PSQL], stdout=log_file)
        log_file.close()

    except CalledProcessError as e:
        print(e.returncode)


# main
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PG helper')
    parser.add_argument('-r', '--restart',
                        help='restart PG', action='store_true')
    parser.add_argument('-d', '--build',
                        help='disable build', action='store_true')
    parser.add_argument('-t', '--terminal',
                        help='start terminal', action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='verbose mode', action='store_true')
    parser.add_argument('-s', '--start',
                        help='start PG', action='store_true')
    parser.add_argument('-k', '--kill',
                        help='kill PG', action='store_true')
    parser.add_argument('-o', '--only_build',
                        help='only build PG', action='store_true')

    args = parser.parse_args()

    # Verbosity
    verbose_mode = False
    if args.verbose:
        verbose_mode = True

    # PG
    build_mode = True
    if args.build:
        build_mode = False

    if build_mode:
        execute_pg_build(verbose_mode)

    # PG_CTL
    pg_ctl_mode = 1  # restart mode

    if args.start:
        pg_ctl_mode = 2  # start mode
    if args.kill:
        pg_ctl_mode = 3  # kill mode

    if args.only_build is False:
        execute_pg_ctl(pg_ctl_mode)

    # PSQL
    terminal_mode = False

    if args.terminal:
        terminal_mode = True
    if terminal_mode:
        execute_psql()
