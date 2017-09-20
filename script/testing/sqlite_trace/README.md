# SQLite Trace Testing

This script will download the SQLite query trace file and run it with our testing framework. It will run each query in Postgres and Peloton, and check to make sure that their results match.

This is meant to be run in Jenkins as part of our build/test process.

## Configuration

* Keyword Filter File: Any query in the trace files will be ignored if it contains any keyword listed in this file.

* Supported Trace Files: A list trace files that will be used in the test. As we expand support for new SQL features, we should include new files.

## Instructions

Note that `run.sh` will do these steps automatically.

1. Run `clear.sh` to clear the current workspace.
2) Download peloton-test (temporarily from my forked version)
3) Download sqlite trace files
4) Run `test.py`. This will first convert the Sqlite trace file into a SQL file format that is needed by the peloton-test framework. It will then invoke the test.
