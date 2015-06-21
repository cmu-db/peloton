#!/usr/bin/python

## ==============================================
## GOAL : Push all branches to github
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
SRC_DIR = reduce(os.path.join, [BUILD_DIR, "src"])
TOOLS_DIR = reduce(os.path.join, [BUILD_DIR, "tools"])

initdb = os.path.join(TOOLS_DIR, "initdb")
pg_ctl = os.path.join(TOOLS_DIR, "pg_ctl")

## ==============================================
## UTILS
## ==============================================
def exec_cmd(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)

    proc = Popen(args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode

    print(out)
    print(err)
    sys.stdout.flush()
    assert(exitcode == 0)

branches=[
"origin/boot", "origin/ddl", "origin/fork2", "origin/imcs", "origin/mover", "origin/perf", "origin/reorg", "origin/stats",
"origin/types", "origin/bridge", "origin/dynamic", "origin/format", "origin/logging", "origin/mvcc",
"origin/plantree", "origin/shm", "origin/storage", "origin/valgrind", "origin/case", "origin/executor", "origin/guc",
"origin/master", "origin/nestloopindex", "origin/plantype", "origin/shmcore", "origin/tbb", "origin/varlen", "origin/cpp",
"origin/fork", "origin/mm", "origin/osx", "origin/plog", "origin/shm_temp", "origin/type", "origin/wflag"
]


## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':
    LOG.info("Pushing branches")

    for branch in branches:
        branch_sub_list = branch.split("/")
        branch_sub = branch_sub_list[1]

        LOG.info("Pushing branch :: " + branch)

        # First, checkout
        cmd = "git checkout " + branch
        exec_cmd(cmd)

        # Another checkout
        cmd = "git checkout " + branch_sub
        exec_cmd(cmd)

        # Finally, push
        cmd = "git push origin"
        exec_cmd(cmd)
