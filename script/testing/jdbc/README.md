## Simple JDBC test for Peloton

The java program here provides simple JDBC operation tests for Peloton
database.

To run the test:
1. Start peloton. Typically, locally, so hostname would be localhost
and if default port would be 15721

2. Compile and run the test:
test_jdbc.sh <type> <hostname> <port>

e.g. test_jdbc.sh PelotonBasicTest localhost 15721

Where 'type' is one of:
      PelotonBasicTest 
      PelotonErrorTest
      PelotonTypeTest
      StocklevelTest
      SSLTest

The program performs the following tests:

> 1. Drop testing table if exsits.
> 2. Create testing table.
> 3. Insert a bunch of tuple using prepared statement.
> 3. Index scan using prepared statement.
