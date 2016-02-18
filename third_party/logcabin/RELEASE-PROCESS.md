This file describes the procedure for creating a new release of LogCabin. For
release notes, see [RELEASES.md](RELEASES.md).


Release Process
===============

- Run pre-commit hooks on all supported compilers (use `scripts/hookmatrix.sh`)
  in both DEBUG and RELEASE mode
  - All hooks should pass, except for possibly spurious timing-sensitive failures

- Run unit tests under valgrind:
  - TimingSensitive tests may fail but all others should pass.
  - The main process heap should have 0 bytes in use at exit and no other
    valgrind errors shown.
  - Death tests may leak memory.

- Run smoke test with servers under valgrind:
  - `./scripts/smoketest.py --binary='valgrind build/LogCabin'`
  - Increase election timeout in `smoketest.conf` as needed to 5 seconds or so.
  - Bootstrap heap should have 0 bytes in use at exit and no other valgrind
    errors shown.
  - All servers' main process heaps should have 0 bytes in use at exit and no
    other valgrind errors shown.
  - Child processes (for snapshots) may leak memory.

- Run smoke test client under valgrind:
  - `./scripts/smoketest.py --client='valgrind build/Examples/SmokeTest'`
  - Heap should have 0 bytes in use at exit and no other valgrind errors shown.

- Run smoke test using g++ 4.9 ThreadSanitizer:
  - Here's a `Local.sc`:
```
CXX='g++-4.9'
CXXFLAGS=['-Werror', '-fsanitize=thread']
LINKFLAGS=['-fsanitize=thread', '-pie']
BUILDTYPE='DEBUG'
```
  - This may print warnings, but look through them.
  - As of 1.0 release, see one warning about a read of 8 bytes in
    LogCabin::Storage::FilesystemUtil::write called from
    LogCabin::Storage::SegmentedLog::segmentPreparerMain().
    This might be a false alarm.

- Run build, unit tests, readme.sh, and `scons rpm` on RHEL/CentOS 6.
  - All should pass.

- Run `scripts/failovertest.py` and
  `scripts/failovertest.py --client=build/Examples/ReconfigureTest`.
  - Should reach the timeout with no errors.

- Run `./scripts/smoketest.py --client='build/Examples/Benchmark
  --writes=1000000 --thread=16' --timeout=45`
  - Should reach timeout with no errors.
  - No performance targets at the moment, but 1.0 on /dev/shm on Diego's laptop
    wrote 3850 objects per second with the default settings (1.1 did the same
    under g++-4.4-release build).

- In `SConstruct`, update the version number and RPM version number and set the
  RPM release string to '1'.

- Run `scons rpm` to verify the RPM version number

- Update `RELEASES.md` to describe the released version

- Create a git commit

- Create a signed git tag named vMAJOR.MINOR.PATCH

- In `SConstruct`, update the version number to MAJOR.MINOR.(PATCH+1)-alpha.0,
  set the RPM version number to MAJOR.MINOR.(PATCH+1), and set the RPM release
  string to '0.1.alpha.0'.

- Update `RELEASES.md` to include a header for the next version

- Create another git commit
