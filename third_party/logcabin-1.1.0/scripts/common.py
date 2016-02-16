#!/usr/bin/env python
# Copyright (c) 2010 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""Misc utilities and variables for Python scripts."""

import contextlib
import os
import random
import re
import shlex
import signal
import subprocess
import sys
import time

__all__ = ['sh', 'captureSh', 'Sandbox', 'getDumpstr']

def sh(command, bg=False, **kwargs):
    """Execute a local command."""

    kwargs['shell'] = True
    if bg:
        return subprocess.Popen(command, **kwargs)
    else:
        subprocess.check_call(command, **kwargs)

def captureSh(command, **kwargs):
    """Execute a local command and capture its output."""

    kwargs['shell'] = True
    kwargs['stdout'] = subprocess.PIPE
    p = subprocess.Popen(command, **kwargs)
    output = p.communicate()[0]
    if p.returncode:
        raise subprocess.CalledProcessError(p.returncode, command)
    if output.count('\n') and output[-1] == '\n':
        return output[:-1]
    else:
        return output

class Sandbox(object):
    """A context manager for launching and cleaning up remote processes."""
    class Process(object):
        def __init__(self, host, command, kwargs, sonce, proc, ignoreFailures):
            self.host = host
            self.command = command
            self.kwargs = kwargs
            self.sonce = sonce
            self.proc = proc
            self.ignoreFailures = ignoreFailures

        def __repr__(self):
            return repr(self.__dict__)

    def __init__(self):
        self.processes = []

    def rsh(self, host, command, ignoreFailures=False, bg=False, **kwargs):
        """Execute a remote command.

        @return: If bg is True then a Process corresponding to the command
                 which was run, otherwise None.
        """
        if bg:
            sonce = ''.join([chr(random.choice(range(ord('a'), ord('z'))))
                             for c in range(8)])
            # Assumes scripts are at same path on remote machine
            sh_command = ['ssh', host,
                          '%s/regexec' % scripts_path, sonce,
                          os.getcwd(), "'%s'" % command]
            p = subprocess.Popen(sh_command, **kwargs)
            process = self.Process(host, command, kwargs, sonce,
                                   p, ignoreFailures)
            self.processes.append(process)
            return process
        else:
            self.rsh(host, command, ignoreFailures, bg=True,
                     **kwargs).proc.wait()
            self.checkFailures()
            return None

    def kill(self, process):
        """Kill a remote process started with rsh().

        @param process: A Process corresponding to the command to kill which
                        was created with rsh().
        """
        killer = subprocess.Popen(['ssh', process.host,
                                   '%s/killpid' % scripts_path,
                                    process.sonce])
        killer.wait()
        try:
            process.proc.kill()
        except:
            pass
        process.proc.wait()
        self.processes.remove(process)

    def restart(self, process):
        self.kill(process)
        self.rsh(process.host, process.command, process.ignoreFailures, True, **process.kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        with delayedInterrupts():
            killers = []
            for p in self.processes:
                # Assumes scripts are at same path on remote machine
                killers.append(subprocess.Popen(['ssh', p.host,
                                                 '%s/killpid' % scripts_path,
                                                 p.sonce]))
            for killer in killers:
                killer.wait()
        # a half-assed attempt to clean up zombies
        for p in self.processes:
            try:
                p.proc.kill()
            except:
                pass
            p.proc.wait()

    def checkFailures(self):
        """Raise exception if any process has exited with a non-zero status."""
        for p in self.processes:
            if (p.ignoreFailures == False):
                rc = p.proc.poll()
                if rc is not None and rc != 0:
                    print ('Process exited with status %d (%s)' %
                           (rc, p.command))
                    raise subprocess.CalledProcessError(rc, p.command)

@contextlib.contextmanager
def delayedInterrupts():
    """Block SIGINT and SIGTERM temporarily."""
    quit = []
    def delay(sig, frame):
        if quit:
            print ('Ctrl-C: Quitting during delayed interrupts section ' +
                   'because user insisted')
            raise KeyboardInterrupt
        else:
            quit.append((sig, frame))
    sigs = [signal.SIGINT, signal.SIGTERM]
    prevHandlers = [signal.signal(sig, delay)
                    for sig in sigs]
    try:
        yield None
    finally:
        for sig, handler in zip(sigs, prevHandlers):
            signal.signal(sig, handler)
        if quit:
            raise KeyboardInterrupt(
                'Signal received while in delayed interrupts section')

# This stuff has to be here, rather than at the beginning of the file,
# because config needs some of the functions defined above.
from config import *
import config
__all__.extend(config.__all__)

def getDumpstr():
    """Returns an instance of Dumpstr for uploading reports.

    You should set dumpstr_base_url in your config file if you want to use this
    to upload reports. See the dumpstr README for more info. You might be able
    to find that README on the web at
    https://github.com/ongardie/dumpstr/blob/master/README.rst
    """

    from dumpstr import Dumpstr
    try:
        url = config.dumpstr_base_url
    except AttributeError:
        d = Dumpstr("")
        def error(*args, **kwargs):
            raise Exception(
                "You did not set your dumpstr_base_url "
                "in localconfig.py, so you can't upload reports.")
        d.upload_report = error
        return d
    else:
        return Dumpstr(url)
