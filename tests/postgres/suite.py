#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Test initdb, pg_ctl start and stop
## ==============================================

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import sys
import shlex
import shutil
import tempfile
import os
import time
import logging
import platform
import unittest
import xmlrunner

from subprocess import Popen, PIPE

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
BUILD_DIR = reduce(os.path.join, [ROOT_DIR, "build"])

# on Jenkins, we do not build in 'build' dir
if platform.node() == 'jenkins':
    TOOLS_DIR = reduce(os.path.join, [ROOT_DIR, "tools"])
    SRC_DIR = reduce(os.path.join, [ROOT_DIR, "src"])
else:
    TOOLS_DIR = reduce(os.path.join, [BUILD_DIR, "tools"])
    SRC_DIR = reduce(os.path.join, [BUILD_DIR, "src"])

LIB_DIR = reduce(os.path.join, [SRC_DIR, ".libs"])
my_env = os.environ.copy()
my_env['LD_LIBRARY_PATH'] = LIB_DIR

initdb = os.path.join(TOOLS_DIR, "initdb")
pg_ctl = os.path.join(TOOLS_DIR, "pg_ctl")

## ==============================================
## Test cases
## ==============================================
class BasicTest(unittest.TestCase):

    def setUp(self):
        LOG.info("Kill previous Peloton")
        cmd = 'pkill -9 "peloton"'
        self.exec_cmd(cmd, False)

        LOG.info("Creating symbolic link for peloton")
        cmd = 'ln -s ' + LIB_DIR + "/peloton " + TOOLS_DIR + "/.libs/"
        self.exec_cmd(cmd, False)

        LOG.info("Setting up temp data dir")
        self.temp_dir_path = tempfile.mkdtemp()
        LOG.info("Temp data dir : %s" % (self.temp_dir_path))

    def test_basic(self):
        LOG.info("Bootstrap data dir using initdb")
        cmd = initdb + ' ' + self.temp_dir_path
        self.exec_cmd(cmd)

        LOG.info("Starting the Peloton server")
        cmd = pg_ctl + ' -D ' + self.temp_dir_path + ' -l '+ self.temp_dir_path + '/basic_test_logfile start'
        self.exec_cmd(cmd)

        LOG.info("Waiting for the server to start")
        time.sleep(5)

        LOG.info("Stopping the Peloton server")
        cmd = pg_ctl + ' -D ' + self.temp_dir_path + ' -l '+ self.temp_dir_path+'/basic_test_logfile stop'
        self.exec_cmd(cmd)

        LOG.info("Starting the Peloton server in TEST MODE")
        cmd = pg_ctl + ' -D ' + self.temp_dir_path + ' -l '+ self.temp_dir_path+'/bridge_test_logfile -o -Z start'
        self.exec_cmd(cmd)

        LOG.info("Waiting for the server to start")
        time.sleep(5)

        LOG.info("Stopping the Peloton server")
        cmd = pg_ctl + ' -D ' + self.temp_dir_path + ' -l '+ self.temp_dir_path+'/bridge_test_logfile stop'
        self.exec_cmd(cmd)

    def exec_cmd(self, cmd, check=True):
        """
        Execute the external command and get its exitcode, stdout and stderr.
        """
        args = shlex.split(cmd)

        proc = Popen(args, stdout=PIPE, stderr=PIPE, env=my_env)
        out, err = proc.communicate()
        exitcode = proc.returncode

        print(out)
        print(err)
        sys.stdout.flush()
        if check:
            self.assertTrue(exitcode == 0)

    def tearDown(self):
        LOG.info("Cleaning up the data dir")
        shutil.rmtree(self.temp_dir_path)

        os.remove(TOOLS_DIR + "/.libs/peloton")


## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':
    unittest.main(
         testRunner=xmlrunner.XMLTestRunner(output='python_tests', outsuffix=""),
        failfast=False, buffer=False, catchbreak=False
    )

