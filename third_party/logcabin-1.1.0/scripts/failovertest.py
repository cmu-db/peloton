#!/usr/bin/env python
# Copyright (c) 2012-2014 Stanford University
# Copyright (c) 2014-2015 Diego Ongaro
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
This runs some basic tests against a LogCabin cluster while periodically
killing servers.

Usage:
  failovertest.py [--client=<cmd>]... [options]
  failovertest.py (-h | --help)

Options:
  -h --help            Show this help message and exit
  --binary=<cmd>       Server binary to execute [default: build/LogCabin]
  --client=<cmd>       Client binary to execute
                       [default: 'build/Examples/FailoverTest']
  --reconf=<opts>      Additional options to pass through to the Reconfigure
                       binary. [default: '']
  --servers=<num>      Number of servers [default: 5]
  --timeout=<seconds>  Number of seconds to wait for client to complete before
                       exiting with an ok [default: 20]
  --killinterval=<seconds>  Number of seconds to wait between killing servers
                            [default: 5]
  --launchdelay=<seconds>  Number of seconds to wait before restarting server
                           [default: 0]
"""

from __future__ import print_function, division
from common import sh, captureSh, Sandbox, smokehosts
from docopt import docopt
import os
import random
import subprocess
import time

def main():
    arguments = docopt(__doc__)
    client_commands = arguments['--client']
    server_command = arguments['--binary']
    num_servers = int(arguments['--servers'])
    reconf_opts = arguments['--reconf']
    if reconf_opts == "''":
        reconf_opts = ""
    timeout = int(arguments['--timeout'])
    killinterval = int(arguments['--killinterval'])
    launchdelay = int(arguments['--launchdelay'])

    server_ids = range(1, num_servers + 1)
    cluster = "--cluster=%s" % ','.join([h[0] for h in
                                        smokehosts[:num_servers]])
    with Sandbox() as sandbox:
        sh('rm -rf smoketeststorage/')
        sh('rm -f debug/*')
        sh('mkdir -p debug')

        for server_id in server_ids:
            host = smokehosts[server_id - 1]
            with open('smoketest-%d.conf' % server_id, 'w') as f:
                try:
                    f.write(open('smoketest.conf').read())
                    f.write('\n\n')
                except:
                    pass
                f.write('serverId = %d\n' % server_id)
                f.write('listenAddresses = %s\n' % host[0])


        print('Initializing first server\'s log')
        sandbox.rsh(smokehosts[0][0],
                    '%s --bootstrap --config smoketest-%d.conf' %
                    (server_command, server_ids[0]),
                   stderr=open('debug/bootstrap', 'w'))
        print()

        processes = {}

        def launch_server(server_id):
            host = smokehosts[server_id - 1]
            command = ('%s --config smoketest-%d.conf -l %s' %
                       (server_command, server_id, 'debug/%d' % server_id))
            print('Starting %s on %s' % (command, host[0]))
            processes[server_id] = sandbox.rsh(
                host[0], command, bg=True)
            sandbox.checkFailures()

        for server_id in server_ids:
            launch_server(server_id)

        print('Growing cluster')
        sh('build/Examples/Reconfigure %s %s set %s' %
           (cluster,
            reconf_opts,
            ' '.join([h[0] for h in smokehosts[:num_servers]])))

        for i, client_command in enumerate(client_commands):
            print('Starting %s %s on localhost' % (client_command, cluster))
            sandbox.rsh('localhost',
                        '%s %s' % (client_command, cluster),
                        bg=True,
                        stderr=open('debug/client%d' % i, 'w'))

        start = time.time()
        lastkill = start
        tolaunch = [] # [(time to launch, server id)]
        while True:
            time.sleep(.1)
            sandbox.checkFailures()
            now = time.time()
            if now - start > timeout:
                print('Timeout met with no errors')
                break
            if now - lastkill > killinterval:
                server_id = random.choice(processes.keys())
                print('Killing server %d' % server_id)
                sandbox.kill(processes[server_id])
                del processes[server_id]
                lastkill = now
                tolaunch.append((now + launchdelay, server_id))
            while tolaunch and now > tolaunch[0][0]:
                launch_server(tolaunch.pop(0)[1])

if __name__ == '__main__':
    main()
