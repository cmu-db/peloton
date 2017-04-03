## Simple JDBC test for Peloton

The java program here provides simple JDBC operation tests for Peloton database. To run the test, simply type:

test_jdbc.sh <type>

Where 'type' is either 'basic', 'stats', or 'copy'

The program performs the following tests:

> 1. Drop testing table if exsits.
> 2. Create testing table.
> 3. Insert a bunch of tuple using prepared statement.
> 3. Index scan using prepared statement.
