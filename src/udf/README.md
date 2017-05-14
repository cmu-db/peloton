# UDF

This directory contains source file for implementing Peloton's User-Defined Function utility and related utilities.

Overview
===========
(TODO: PP and Nasrin)
We build the UDF feature for Peloton from scratch. Similar to pg\_proc in postgres, We implemented the function\_catalog to register UDF. We implement our own lex and yacc files for UDF grammer, which supports return-stmt and if-else-stmt. Current UDF implementation is able to do basic operation on both scalar and table column as well as if-else and recursive examples, which are shown in our final presentation.

Function Catalog
-------------
(TODO: PP and Nasrin)

UDF Execution
-------------
UDF execution is executed in an interpreted way. Take "if-else" as an example, the condition is evaluated at first and runs the either of the branches after the result of condition is returned.

Further Work
===========

We suggests a list of future work for incoming students/interns to work on UDF.

Parsing
-------------
* Both Lex and Yacc files are yet simple, i.e. only recognizes a few tokens and supports limited rules. One needs to add more rules for richer utilities.
* Add compilation instructions for Lex and Yacc files to CMAKE. Currently, yex and yacc file are NOT tranformed to cpp files along with makefile.

Registration
-------------
* No validation is done in registration yet.
(TODO: PP and Nasrin)

Execution
-------------
* Only return-stmt and if-else-stmt are supported now. One could supports other stmts, e.g. for-loop-stmt, by adding its stmt class.
* The invocation of sql expression is done through traffic\_cop\_.ExecuteStatement(), which involves parse, plan and execution stages. However, as sql expression is always the same, the results of parse and plan are identical. An optimization is to do parse and plan in construction and invocation only triggers execution.
