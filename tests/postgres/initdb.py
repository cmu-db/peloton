#!/usr/bin/env python
# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import sys
import shlex
import shutil
import tempfile
import os

from subprocess import Popen, PIPE

def get_exitcode_stdout_stderr(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)

    proc = Popen(args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    #
    return exitcode, out, err

initdb = 'initdb'
temp_dir_path = tempfile.mkdtemp()
print(temp_dir_path)

cmd = initdb + ' ' + temp_dir_path
exitcode, out, err = get_exitcode_stdout_stderr(cmd)

shutil.rmtree(temp_dir_path)

print(out)
print(err)

sys.exit(exitcode)
