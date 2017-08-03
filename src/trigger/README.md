# Triggers
Major updates for triggers are:
- Support execute **create** and **drop** trigger statements
  - Update data structure for statement to support trigger
  - Convert Postgres parsing tree for Peloton statement for trigger
  - Update data structure for plan to support trigger
  - Convert state to plan for trigger
- Data structures for Trigger and TriggerList and related operations
- Put trigger information into the catalog and support updates *(Not all trigger predicates are supported because of the complexity in serialization and deserialization. We currently support binary operator for common types. e.g. `NEW.income > OLD.income`, `NEW.income > 3`. This could be improved by implementing serialization and deserialization of each kind of node in expression tree.)*
- Update DataTable with TriggerList information
- Evaluate the predicates in the trigger
- Invoke UDFs and send contexts *(It takes a log of all data needed for a UDF call (function name, arguments, old/new tuples, etc.), but does not really call UDFs. One concern is that UDF is not supported in the master branch currently. Another concern is that currently UDF is mainly designed for read-only operations without SQL statements, but mostly, functions invoked by a trigger need to apply SQL statements on databases. Hope `ExecCallTriggerFunc` could be truly implemented after these problems are resolved.)*
- Trigger operations for 12 kinds of triggers (before/after, insert/delete/update, row/statement)
