#!/usr/bin/env python
# Copyright (c) 2011 Stanford University
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

"""
This module defines a collection of variables that specify site-specific
configuration information such as names of RAMCloud hosts and the location
of RAMCloud binaries.  This should be the only file you have to modify to
run RAMCloud scripts at your site.
"""

from common import captureSh
import os
import re
import subprocess
import sys

__all__ = ['git_branch',
        'hosts', 'obj_dir', 'obj_path', 'scripts_path',
        'smokehosts',
        'top_path']

# git_branch is the name of the current git branch, which is used
# for purposes such as computing objDir.
try:
    git_branch = re.search('^refs/heads/(.*)$',
                           captureSh('git symbolic-ref -q HEAD 2>/dev/null'))
except subprocess.CalledProcessError:
    git_branch = None
    obj_dir = 'obj'
else:
    git_branch = git_branch.group(1)
    obj_dir = 'obj.%s' % git_branch

# obj_dir is the name of the directory containing binaries for the current
# git branch (it's just a single name such as "obj.master", not a full path)
obj_dir = 'build'

# The full path name of the directory containing this script file.
scripts_path = os.path.dirname(os.path.abspath(__file__))

# The full pathname of the parent of scriptsPath (the top-level directory
# of a RAMCloud source tree).
top_path = os.path.abspath(scripts_path + '/..')

# Add /usr/local/lib to LD_LIBARY_PATH it isn't already there (this was
# needed for CentOS 5.5, but should probably be deleted now).
try:
    ld_library_path = os.environ['LD_LIBRARY_PATH'].split(':')
except KeyError:
    ld_library_path = []
if '/usr/local/lib' not in ld_library_path:
    ld_library_path.insert(0, '/usr/local/lib')
os.environ['LD_LIBRARY_PATH'] = ':'.join(ld_library_path)

# All of the hosts available for servers or clients; each entry
# consists of a name for the host (for ssh), an IP address
# to use for creating service locators. and an id for generating
# Ethernet addresses.
hosts = []
for i in range(1, 61):
    hosts.append(('rc%02d' % i,
                  '192.168.1.%d' % (100 + i),
                  i))

# The set of hosts available for basic correctness testing.
smokehosts = hosts

# Full path to the directory containing RAMCloud executables.
obj_path = '%s/%s' % (top_path, obj_dir)

# Try to include local overrides.
try:
    from localconfig import *
except:
    pass
