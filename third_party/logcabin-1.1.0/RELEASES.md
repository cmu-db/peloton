Versioning
==========

LogCabin uses [SemVer](http://semver.org) for its version numbers. Its "public
API" consists of many components, including its network and disk formats, its
client library, and the command line arguments for various executables. These
components are all released together under a single version number, and the
release notes below describe which components have actually changed.

Release Process
===============

See [RELEASE-PROCESS.md](RELEASE-PROCESS.md).

Version 1.1.0 (2015-07-26)
==========================

This release brings many bug fixes and improvements since v1.0.0. All users are
strongly encouraged to upgrade.

Bug fixes (high severity):

- Fixes packaging up very large AppendEntries requests. Before, it was possible
  for a leader to send a non-contiguous list of entries to the follower, and
  the follower would end up with a corrupt log (issue #160). Before, it was
  also possible for packing up the requests to take so long as to cause
  availability and performance problems (issue #161).
- Fixes occasional hang when exiting due to unsafe access of a boolean flag
  (issue #144).
- Fixes hang when exiting while removing servers from the cluster configuration
  (issue #183).
- Fixes client waiting past its timeout on another client's connection attempt
  (issue #173).

Bug fixes (low severity):

- Fixes `Core::Debug::DebugMessage` move constructor, where `processName` and
  `threadName` were not moved over as they should have been (git 77d7f6b).
- Fixes signed integer overflow bug under aggressive optimizing compilers
  affecting `SteadyTimeConverter`, which is only used in producing ServerStats
  (git 6473400). Turns on `-fno-strict-overflow` compiler setting.
- Fixes repeated PANIC in InstallSnapshot RPC after a server restarts while
  receiving a snapshot (issue #174). This could result in a temporary
  availability issue that would resolve itself on the next term change.
- Fixes failing conditional tree operations after setting a condition with a
  relative path (issue #177).
- Fixes event loop thread dumping server stats to the debug log, which had the
  potential for delays and deadlock (we never saw deadlock occur in practice,
  however). Now the stats are dumped from a separate thread (issue #159).
- Fixes PANIC due to "No route to host" error after many minutes of the network
  interface going down (issue #154). This is considered low severity as it was
  unlikely to affect overall cluster availability.

Internal improvements:

- Adds gcc 5.1 (which required no changes; issue #141) and clang 3.4, 3.5, 3.6,
  and 3.7 (issue #9) as supported compilers.
- `liblogcabin.a` is now compiled with `-fPIC`, so it can be linked into shared
  object (.so) files (git 1ca169c).
- Optimizes setting `nextIndex` on leaders by capping it to just past the
  follower's last log index. This helps with followers that are new or have
  fallen far behind (git fcbacbb).
- Clients now make a best effort attempt to close their sessions when they shut
  down gracefully (issue #116). Before, client sessions were only ever expired
  after a timeout. This state could accumulate quickly when running short-lived
  clients in a tight loop. Enabling this change requires all servers to be
  updated (so that the state machine is updated); new clients talking to old
  clusters will issue a warning that they are unable to close their sessions.
- SegmentedLog now coalesces back-to-back fdatasync calls, making batch log
  appends much more efficient (issue #165).
- Leaders will now limit the amount of data they send to a follower when their
  connection to that follower is lost, which reduces wasted bandwidth
  (git e238fa6).

New backwards-compatible changes:

- `LogLevel`, `getLogFilename`, `setLogFilename`, `reopenLogFromFilename`,
  `getLogPolicy`, `logPolicyFromString`, `logPolicyToString` were introduced in
  `include/LogCabin/Debug.h`.
- The LogCabin daemon will now reopen its log file on `SIGUSR2` (useful for log
  rotation; issue #150). Signal handling was not listed as part of LogCabin's
  public API until now; signals listed in `--help` messages are now subject to
  semantic versioning.
- The `build/Examples/ServerControl` or `/usr/bin/logcabinctl` program can be
  used to inspect and manipulate an individual server's state (issue #151). Its
  command line is now part of LogCabin's public API.
- All clients now have `--verbose` and `--verbosity` to control the debug log
  level and policy (issue #153). The server's config file now has a new option
  `logPolicy` to control the same.
- Exceptions due to bad user input are now caught by broad exception handlers.
  They were uncaught before, causing the process to abort() and create
  unnecessary core files (issue #166).
- Adds several new ServerStats metrics.

Changes to unstable APIs:

- `build/Examples/ServerStats` or `/usr/bin/logcabin-serverstats` along with
  `scripts/serverstats.py` have been removed. The `logcabinctl` program can now
  be used to fetch the stats instead, and the Python wrapper wasn't kept
  up-to-date anyhow. External clients linked to old versions of
  `LogCabin::Client::Cluster::getServerStats()` will not work with new servers,
  and `logcabinctl stats get` and other clients linked to the same function will
  not work with old servers.
- `Examples/HelloWorld` is no longer installed to
  `/usr/bin/logcabin-helloworld`.
- Fixes RPM build on Scons 2.3.0 (git e77d217).
- Removes the 'reload' command from the Red Hat init script, which would
  previously kill the server (git 28044cb).
- Changes the log path in the Red Hat init script to
  `/var/log/logcabin/logcabin.log` (git e9e466f).
- Changes the RPM package and Red Hat init script to launch the LogCabin server
  as user `logcabin` instead of `root` (issue #178). Changes the storage path.
- The client library in testing mode (`MockClientImpl`) will now return obvious
  timeout errors, making it easier to unit test client code (git a8b22be).

Version 1.0.0 (2015-04-29)
==========================

This is the first stable release of LogCabin. We encourage others to try this
release out, and we believe it to be ready for production use. As it is the
very first release, users are advised to check back frequently in case serious
bugs are found.

The public API with respect to versioning consists of the following:

- `include/LogCabin/Client.h`: API
- `include/LogCabin/Debug.h`: API
- `build/LogCabin` or `/usr/bin/logcabind`: command line
- `build/Examples/Reconfigure` or `/usr/bin/logcabin-reconfigure`: command line
- `build/Examples/TreeOps` or `/usr/bin/logcabin`: command line
- config file format and options: defined by `sample.conf`
- client-to-server network protocol: compatibility
- server-to-server network protocol: compatibility
- replicated state machine behavior: compatibility
- storage layout on disk: compatibility
- snapshot format on disk: compatibility
- log format on disk of `Segmented` storage module: compatibility

Command line APIs consist of argv, zero vs nonzero exit statuses, and side
effects, but not necessarily stdout and stderr. These are documented with `-h`
and `--help` flags.

The interfaces/protocols indicating "compatibility" are not documented
publicly, but interoperability with different versions of the code is
maintained. Interoperability with third-party implementations is not
guaranteed, as there is no explicit protocol specification.

- Network protocols indicating "compatibility" provide forwards and backwards
  compatibility: older code and newer code must be able to interoperate within
  a MAJOR release. This is desirable in the network protocols to allow
  non-disruptive rolling upgrades.

- Disk formats indicating "compatibility" provide backwards compatibility:
  newer code must be able to accept formats produced by older code within a
  MAJOR release. However, older code may not be able to accept disk formats
  produced by newer code. This reflects the expectation that servers will be
  upgraded monotonically from older to newer versions but never back.

- The replicated state machine (which contains the core Tree data structure
  that clients interact with, among other things) provides backwards
  compatibility for a limited window of time. LogCabin will only update the
  externally visible behavior of its replicated state machine when all
  currently known servers support the new version. At that point, servers
  running the old version of the code may not be able to participate in the
  cluster (they will most likely PANIC repeatedly until their code is
  upgraded).


The following are specifically excluded from the public API and are not subject
to semantic versioning (they may be added to the public API in future
releases):

- client library ABI
- `scripts/logcabin-init-redhat` command line
- various other scripts
- `build/Examples/ServerStats` command line
- various other `build/Examples` executables
- `build/Storage/Tool` command line
- `ServerStats` ProtoBuf fields
- log format on disk of `SimpleFile` storage module
