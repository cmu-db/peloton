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

## History

* 2017-02-02: Initial naming convention written by Andy