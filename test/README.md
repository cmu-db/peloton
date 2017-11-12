# Peloton Test Directory

This directory contains all of the C++ unit test cases for the DBMS.
The hierarchy in this directory should be a mirror of the `src` structure.

If you add a utility class to the test directory, you must edit `CMakeLists.txt` to include its .cpp file in the `peloton-test-common` library.

## Naming Conventions

* A suite of unit tests for some particular feature or component of the system should be stored in a single source file. That file needs to end with the suffix `_test.cpp` in order to automatically get picked up as a unit test.

   **Example:** `string_util_test.cpp`.  

* The name of a test suite class should end with the suffix `Tests`.

   **Example:** `StringFunctionsTests`.  

* The name of each individual test case in a suite class be descriptive and explain what the test is checking. Avoid generic names like `BasicTest`. The name of the test needs to end with the suffix `Test`.

   **Example:** `StringFunctionsTests::CharLengthTest`  

* Any utility class that is specific to testing should be start with the prefix `Testing`. This is necessary to avoid confusion with the utility classes that are part of the main codebase.

   **Example:** `TestingSQLUtil`.

## Using `TestingExecutorUtil` to create tables

During testing of your functionality, you will most likely need to create a table and check your functionality against the tuples in the table in the testing code. Instead of creating a custom table for your test, make use of the functions in `TestingExecutorUtil (/test/executor/testing_executor_util.cpp)`. A detailed explanation of how to use and create tables for your test by using the `TestingExecutorUtil` is listed below. 

* `CreateAndPopulateTable()`

	This is the most basic function call and in most cases will create the table you want test your functionality against. The function creates a table with 3 tile groups and 5 tuples in each tile group, thus the table has a total of 15 tuples. Each tuple has 4 columns (Column 1 and 2 are integer columns, Column 3 is decimal and Column 4 is VARCHAR). If the row number is `r` , then the tuple will be  `(r*10 , r*10 +1, r*10 +2, r*10 +3)`


* `CreateTable()` and `PopulateTable()`
	
	If `CreateAndPopulateTable()` does not satisfy your requirements you can make use of the following 2 calls:
	* `CreateTable()` : This lets you choose the tuples per tile group , whether you want indexes and the table id for referring to this table. The function returns a `storage::Datatable *`. It has the same 4 columns as described above.
	* `PopulateTable()` : This takes in the table pointer, the current transaction pointer and populates the table for you. You have 4 parameters through which you can control how you want to populate this table.
		* `int num_rows` : You can choose the number of rows in the table using this function
		* `bool mutate` : If this is `true`  each tuple id of the form `(r*30 , r*30 +1, r*30 +2, r*30 +3)` where `r` is the row number.
		* `bool random` : If this `true` each tuple is of the form `(r*10 , rand, rand, rand)` where `rand` is a random number and `r` is the row number.
		* `bool groupby` : If this is `true` only the first column has only 2 distinct values per tile group. Hence each tuple is of the form `(int(r/(num_rows/2)), r*10 +1, r*10 +2, r*10 +3 )` where `r` is the row number and `num_rows` is the total number of rows.


If and only if you feel that none of the functions are able to create a table for your test, add the functionality to create that table in `TestingExecutorUtil` instead of creating a table in your specific test. This will allow others also to reuse this table if they need to.

## History

* 2017-02-02: Initial naming convention written by Andy
* 2017-11-07: Usage of TestingExecutorUtil to create tables written by Rohit
