

This directory contains tests using Junit4 and JDBC and prepared statements.

To compile:
make

To run, using the Junit console:
1. Start up the database server, manually, e.g.
   ./peloton

2. Run the tests, (from this directory):
   make test


If the JUnit console output style is not to your taste,
they can be run more directly using the Junit runner. Copy the following
code into a file, and invoke it from this directory, e.g.
   bash run_tests.sh

# construct the class path
CP=".:lib/hamcrest-core-1.3.jar:lib/postgresql-9.4.1209.jre6.jar:lib/junit-4.12.jar"

# execute the InsertPSTest class. More than one test class may be
# supplied on the command line
java -cp $CP org.junit.runner.JUnitCore InsertPSTest


Test notes
----------

1. The InsertTPCCTest uses a table schema and insert statment from the
   TPPC benchmark.
   Peloton fails this test. It will be enabled once fixed.

2. InsertPSTest contains a set of prepared statement tests. The tests that
   currently do not pass on Peloton are disabled.
   	     
