

This directory contains tests using Junit4 and JDBC.

Note:
- These tests are not yet integrated into the continuous integration
  process (i.e. Travis or Jenkins). They will be integrated as soon
  as feasible.

  In the meantime, run the tests manually. Instructions follow.

Installation and pre-requisites
-------------------------------
You'll need
- java JDK or JRE

Other than that, no explicit installation is required.

The necessary Java libraries supporting these tests are included in the
lib directory.

Running the tests
-----------------

1. Compile the tests.
   make

2. To run the tests, first start Peloton manually in a separate shell, e.g. 
   ./peloton

3. Run the tests, (from this directory):
   make test

Adding new tests
----------------
Add the tests to the Makefile for compilation and execution.

Code structure for tests
------------------------
- PLTestBase contains supporting functions shared across tests. Supplement
  as needed. Avoid duplicating code in tests.

- Add a new XXX.java file for each new test class. Each class may
  contain as many tests as desired. The current tests use
  one schema for the class.

- See UpdateTest.java for an example of a simple test.
  - Each test will need a Connection variable and SQL statements
    to setup the tables.

- See InsertPSTest.java for additional examples, including use
  of prepared statements. Most likely you'll want to implement
  using normal statements rather than prepared statements.

- Setup() is called prior to each test
- Teardown() is called after each test
  If you need different granularity, see the JUnit4 documentation. Class
  level setup / teardown is possible.

- Functions annotated with @Test are the actual tests.
  Tests may be temporarily disabled by commented out the annotation, e.g.
  //@Test.


Alternatives
------------

If the JUnit console output style is not to your taste when running the
tests, they can be run more directly using the Junit runner. Copy the following
code into a file, and invoke it from this directory, e.g.
   bash run_tests.sh

# construct the class path
CP=".:lib/hamcrest-core-1.3.jar:lib/postgresql-9.4.1209.jre6.jar:lib/junit-4.12.jar"

# execute the InsertPSTest class. More than one test class may be
# supplied on the command line
java -cp $CP org.junit.runner.JUnitCore InsertPSTest
