#!/usr/bin/env python
# Copyright (c) 2012 Stanford University
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
This runs a LogCabin cluster and continually kills off the leader, timing how
long each leader election takes.
"""

from __future__ import print_function
from common import sh, captureSh, Sandbox, hosts
import re
import subprocess
import sys
import time

num_servers = 5

def same(seq):
    for x in seq:
        if x != seq[0]:
            return False
    return True

def await_stable_leader(sandbox, server_ids, after_term=0):
    while True:
        server_beliefs = {}
        for server_id in server_ids:
            server_beliefs[server_id] = {'leader': None,
                                         'term': None,
                                         'wake': None}
            b = server_beliefs[server_id]
            for line in open('debug/%d' % server_id):
                m = re.search('All hail leader (\d+) for term (\d+)', line)
                if m is not None:
                    b['leader'] = int(m.group(1))
                    b['term'] = int(m.group(2))
                    continue
                m = re.search('Now leader for term (\d+)', line)
                if m is not None:
                    b['leader'] = server_id
                    b['term'] = int(m.group(1))
                    continue
                m = re.search('Running for election in term (\d+)', line)
                if m is not None:
                    b['wake'] = int(m.group(1))
        terms = [b['term'] for b in server_beliefs.values()]
        leaders = [b['leader'] for b in server_beliefs.values()]
        if same(terms) and terms[0] > after_term:
            assert same(leaders), server_beliefs
            return {'leader': leaders[0],
                    'term': terms[0],
                    'num_woken': sum([1 for b in server_beliefs.values() if b['wake'] > after_term])}
        else:
            time.sleep(.25)
            sandbox.checkFailures()

with Sandbox() as sandbox:
    sh('rm -f debug/*')
    sh('mkdir -p debug')

    server_ids = range(1, num_servers + 1)
    servers = {}

    def start(server_id):
        host = hosts[server_id - 1]
        command = 'build/LogCabin -i %d' % server_id
        print('Starting LogCabin -i %d on %s' % (server_id, host[0]))
        server = sandbox.rsh(host[0], command, bg=True,
                             stderr=open('debug/%d' % server_id, 'w'))
        servers[server_id] = server

    for server_id in server_ids:
        start(server_id)

    num_terms = []
    num_woken = []
    for i in range(100):
        old = await_stable_leader(sandbox, server_ids)
        print('Server %d is the leader in term %d' % (old['leader'], old['term']))

        print('Killing server %d' % old['leader'])
        sandbox.kill(servers[old['leader']])
        servers.pop(old['leader'])
        server_ids.remove(old['leader'])
        new = await_stable_leader(sandbox, server_ids, after_term=old['term'])
        print('Server %d is the leader in term %d' % (new['leader'], new['term']))
        sandbox.checkFailures()
        num_terms.append(new['term'] - old['term'])
        print('Took %d terms to elect a new leader' % (new['term'] - old['term']))
        num_woken.append(new['num_woken'])
        print('%d servers woke up' % (new['num_woken']))

        server_ids.append(old['leader'])
        start(old['leader'])
    num_terms.sort()
    print('Num terms:',
          file=sys.stderr)
    print('\n'.join(['%d: %d' % (i + 1, term) for (i, term) in enumerate(num_terms)]),
          file=sys.stderr)
    num_woken.sort()
    print('Num woken:',
          file=sys.stderr)
    print('\n'.join(['%d: %d' % (i + 1, n) for (i, n) in enumerate(num_woken)]),
          file=sys.stderr)
