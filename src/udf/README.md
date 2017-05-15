# UDF

This directory contains source file for implementing Peloton's User-Defined Function utility and related utilities.

Overview
===========
We have added suppport for registering PL\`PGSQL. This includes making changes in the Parser, Optimizer and Executor. Similar to pg\_proc in postgres, we have implemented the function\_catalog which is where the registered functions reside. We have similarly also added support to invoke UDFs. We implement our own lex and yacc files for UDF grammer, which supports return-stmt and if-else-stmt. Current UDF implementation is able to do basic operation on both scalar and table column as well as if-else and recursive examples, which are shown in our final presentation.

Function Catalog
-------------
The function catalog acts a registry for all user defined functions. The structure of the catalog can be best understood by referring to https://www.postgresql.org/docs/9.3/static/catalog-pg-proc.html.
  We provide two indexes to scan the catalog - A primary index on the function_oid and a seconday index on the function_name. 

UDF Execution
-------------
UDF execution is executed in an interpreted way. Take "if-else" as an example, the condition is evaluated at first and runs the either of the branches after the result of condition is returned.

Current Issues and Future Work
===========
These are the tasks that can be done incrementally to add additional functionality and make UDF support more robust within Peloton.

UDF Parsing
-------------
* Both Lex and Yacc files are yet simple, i.e. only recognizes a few tokens and supports limited rules. One needs to add more rules for richer utilities.
* Add compilation instructions for Lex and Yacc files to CMAKE. Currently, yex and yacc file are NOT tranformed to cpp files along with makefile.

UDF Registration
-------------
* No validation is done during registration. The function string is just added into the function\_catalog.
* Only PL_PGSQL UDFs are currently supported. Support for C UDFs can be added.
* Built-in functions can also be moved to the function_catalog with language type being set to "internal".

UDF invocation
------------- 
* Currently, checking whether the function is actually present in the catalog is done in the execution stage, instead of the planning stage because we do not have the transaction context. This part can be changed once expressions support transactions.
* Return type is also being hard-coded to INTERGER currently, for the same reason as above.

UDF Handler
-------------
* Only return-stmt and if-else-stmt are supported now. One could supports other stmts, e.g. for-loop-stmt, by adding its stmt class.
* The invocation of sql expression is done through traffic\_cop\_.ExecuteStatement(), which involves parse, plan and execution stages. However, as sql expression is always the same, the results of parse and plan are identical. An optimization is to do parse and plan in construction and invocation only triggers execution.
* Make the UDF execution faster by reusing cached plans.
