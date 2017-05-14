## Overview
The code adds some basic constraint support to the Peloton database. Our main changes include adding NOT NULL, DEFAULT, CHECK, consolidating PRIMARY KEY and UNIQUE constraints, FOREIGN KEY INSERT/UPDATE check and adding appropriate test cases.

## Changes
The primary changes we have made include:
- ~/src/catalog : The column, constraint and schema definitions have been changed to accommodate for checking the constraints on each tuple.
- ~/src/storage : The data_table.cpp file has been modified to add the new constraint checking routines when allocating a new slot.
- ~/test/catalog : Added new test cases for constraint tests. Note that constraints_tests_utils.cpp should be ignored, refer to constraints_tests_utils.h for implementation.
- ~/src/planner : some files have been changed to support exception handling and default case improvement: plan_executor.cpp and the other executors
- ~/src/parser: create_statement.h
- ~/src/tcop: tcop.cpp are modified for exception handling as well.

## Methodology
Constraint checking / parsing are done mostly in data_table, some of them are doen in corresponding executors.
- NOT NULL: NULL value is checked in data_table.cpp for a given tuple.
- DEFAULT: The AbstractExpression is passed from postgresparser to create_statement and used by create_executor to get the default value associed with the column.
- CHECK: Similar to DEFAULT, the expression passing is done in the create_executor, and in data_table the values are checked.
- FOREIGN KEY: Currently only supports checking keys in the sink table (including visibility check).
- PRIMARY KEY and UNIQUE: Simply index operations. Index creation is done in create_executor, exceptions will be thrown if the constraint is violated during index operations in data_table. 

## Constraint Exception Handling
Whenever a constraint is violated, an exception will be thrown. In plan_executor the exception will be first handled, and then it will be rethrown and caught in tcop (the highest level) where the error message can be set. Currently we only support a single constraint exception type. 

## Tests
We have individual tests for NOT NULL, DEFAULT, CHECK, FOREIGN KEY repectively. They mostly check for query execution correctness and proper exception handling.

## Issues
- Foreign key support with cascade on delete / update is not ready.
- The stats_test may call InsertTuple directly without exception handling, as a result the stats_test could not pass.
- Parser support is mostly ready. Yet some code refactorization / modification may be done in create_statement.h for better modularity, where the parser passes parameters.
- More tests needed, especially multi-threaded test. A thorough comparison with postgresql in terms of constraint checking overhead is needed.