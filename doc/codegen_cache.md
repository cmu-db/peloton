# Codegen Cache and Parameterization

## Overview
This feature provides performance enhancement of the query compilation in Peloton's codegen execution engine, by caching compiled queries and reusing them for the next user requests. The constants and parameters are parameterized, so that the queries with an _similar_ plan would have the benefit of caching.

## Scope
The query cache feature is based on plan/expression/schema/table comparisons.  These classes provide hash functions and equality(and non-equality) operators, in order for the query cache to compare plans using a map.  The plans also provide a function to retrieve parameter values out of them.

The parameterization feature requires extracting parameter information from the plans and sets up some sort of parameter value storage. The codegen translators retrieve the values from this storage at runtime. 

In a brief summary, it contains modifications on the following modules:

### Plan Comparison ###
* *catalog* provides hash/equality checks on `Schema`
* *storage* provides Hash/equality checks on `DataTable`
* *expression* provides Hash/equality checks on expressions such as `AbstractExpression`, `CaseExpression`, `ConstantValueExpression`, `ParameterValueExpression` and `TupleValueExpression`
* *planner* provides Hash/equality checks on plans such as `AggregatePlan`, `DeletePlan`, `HashJoinPlan`, `HashPlan`, `InsertPlan`, `OrderByPlan`, `ProjectInfo`, `ProjectionPlan`, `SeqScanPlan` and `UpdatePlan`

### Parameter Retrieval ###
* *planner* provides functions that retrieves parameter value/information
* *executor* retrieves parameter information and hands it over to codegen
* *codegen* builds up a value cache for the runtime to read the values at execution

### Query Cache and Execution ###
* *execution* checks the query cache before executing a query
* *codegen* provides query cache and parameter value cache. It also adds parameter value translator and changes expression translators to retrieve the cached parameter values

### Refactoring INSERT ###
* *planner* modifies `InsertPlan` to store values in a vector, rather than in `storage::Tuple` when values are provided directly from SQL
* *execution* modifies the legacy `InsertExecutor` to receive a vector of values from the planner, without removing the old way of receiving `storage::Tuple`
* *codegen* is also modified to receive a vector of values, so that the values can be parameterized

## Architectural Design
The overall architecture of Peloton and the entire flow of a query execution remains the same as before, but the exeuction now bypasses the compilation stage when there has been an _equal_ query executed before.  An equal query here is defined as a query with the same plan, but with different constant values and/or different parameter values. The decision, whether the cahced queries are used, is made in `executor::PlanExecutor` before a codegen query execution.

`QueryCache` keeps all the compiled queries in a hash table, which is global in Peloton. A search for an equal query is executed by `Hash()` and `operation==` that are provided by the plan that is going to be compared. Once the equality comparison succeeds, `PlanExecutor` obtains the previously compiled query, a `Query` object, from the cache and this query object is executed through its `Execute()` function. The `Query` object contains a set of compiled functions that can be executed inside LLVM.

`QueryParameters` contains all the parameter information and the constant values by extracting it from provided plan, and also the parameter values from the original parameter value vector in Peloton. This is also achieved before the codegen execution engine is involved.

`ParameterCache` stores the values from `QueryParameters`, which get indexed so that the codegen translators retrieve its actual value by an index. The structure is statically determined at compile time, and dynamically changing the size is not possible in the current design. In other words, the number of parameters are fixed at compile time. This is a reason why we currently have a separate codegen executable for `Insert` with different bulk insert tuple number.

In addition, `InsertPlan` is refactorized not to build a `storage::Tuple` in the case of the query coming in from SQL. It stores values in a vector, and these values in the vector get parameterized.

## Design Rationale and Limitation
The goal of this design is to fit a query cache implemenation smoothly into the existing Peloton architecture.  The logic for plan comparison is implemented in the plans in order for these to be updated whenever there are changes in the plans. 

It does not change a plan in order to optimize the cache performance, i.e. 2+2 is different from 4 in the perspective of `QueryCache`. In addition, it does not re-order the expressions to find a match in the cache. We think that these are a role of the optimizer.

## Testing Plan
### Plan comparisons ###
All the codegen supported queries are tested in `test/planner/planner_equality_test`.  Not all the permutations are tested, but most of the basic and many complex ones are added and tested.

### Parameter Retrieval ###
`test/codegen/parameterizaton_test` contains various parameterization tests.

### Query Cache and Execution ###
`test/codegen/query_cache_test` contains query cache tests including execution.  

### Refactoring Insert ###
`test/sql/insert_sql_test` contains most of the INSERT related SQL tests. `test/codegen/insert_translator` and `test/executor/insert_test` tests the codegen and the old execution engine respectively after refactorization.

## Trade-offs and Potential Problems
The query cache capacity is infinite unless there is a request from outside, setting the capacity. In other words, the query cache itself provides an API to resize the cache, but no admin feature is implemented.

## Future Work
We could pre-populate the cache with some basic and essential queries when a table is generated. One such obvious query is an INSERT, since a data table is useless if there is no tuple in it.

The query cache is global at the current implementation.  In other words, there is one query cache in one Peloton instance.  A query cache can be instantiated for each table, so that it is natural to destroy when a table is destroyed, and the cache can be managed in a more micro manner, e.g. size, turn on/off, etc.

The query cache can be persistently stored in a persistent storage, and retrieved back to the memory cache when Peloton reboots. Checkpointing the cache to the storage in a regular time interval in backgound would suffice.

## Glossary
* Query cache: cache of the query objects with compiled object for codegen execution, which is different from physical plan cache or cache
