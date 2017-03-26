Note - Caching
==============
In order to support caching, we need to build a { plan -> code } map.

However, the current Peloton system doesn't provide plan equality comparison
functionalities.

Therefore, we have to write plan comparison code.

I have listed the expressions and plans that are currently supported by
codegen as well as their major data members.

It looks like that there's no class that can't be compared without a deeper
understanding of Peloton.

So comparison is doable.


Note - Parameterization
=======================
In order to support parameterization, we turn every `ConstantValueExpression`
into a parameter.

During plan equality comparison, we don't compare these parameters.

During codegen, we add new function arguments and pass in these parameters.


Currently supported expressions
===============================

Note that the visitor pattern is supported. We might consider using it.

```
AbstractExpression
|
+- ComparisonExpression (ComparisonTranslator)
|    type  : ExpressionType     # enum
|    left  : AbstractExpression
|    right : AbstractExpression
|
+- OperatorExpression (ArithmeticTranslator)
|    type    : ExpressionType     # enum
|    type_id : type::Type::TypeId # enum
|    left    : AbstractExpression
|    right   : AbstractExpression
|
+- ConstantValueExpression (ConstantTranslator)
|    value : type::Value          # has comparison method
|
+- OperatorUnaryMinusExpression (OperatorUnaryMinusExpression)
|    left : AbstractExpression
|
+- AggregateExpression (Not evaluated, will look into it later)
|    type     : ExpressionType     # enum
|    distinct : bool
|    child    : AbstractExpression
|
+- ConjunctionExpression (ConjunctionTranslator)
|    type  : ExpressionType     # enum
|    left  : AbstractExpression
|    right : AbstractExpression
|
+- TupleValueExpression (TupleValueTranslator)
|    value_idx  : int
|    tuple_idx  : int
|    table_name : string
|    col_name   : string
|    ai         : AttributeInfo # ok
|
+- (Unsupported Expressions)
```

Currently supported plans
=========================

```
AbstractPlan
|
+- AbstractPlan.AbstractScan
|  |
|  +- AbstractPlan.AbstractScan.SeqScanPlan
|  |
|  +- (Unsupported Plans)
| 
+- AbstractPlan.OrderByPlan
|    sort_keys         : [oid_t]
|    descend_flags     : [bool]
|    output_column_ids : [oid_t]
| 
+- AbstractPlan.AggregatePlan
|    project_info       : ProjectInfo
|                           tl  : TargetList    # need AbstractExpression
|                           dml : DirectMapList # ok
|                        
|    predicate          : AbstractExpression   # ok
|    unique_agg_terms   : [AggTerm]
|                            et       : Expression Type    # ok
|                            expr     : AbstractExpression # ok
|                            distinct : bool
|    groupby_col_ids    : [oid_t]              # ok
|    output_schema      : Schema               # ok
|    aggregate_strategy : AggregateType        # ok
| 
+- AbstractPlan.HashPlan
|    hashkeys : [HashKeyPtrType] # need AbstractExpression
| 
+- AbstractPlan.AbstractJoinPlan
|  |
|  +- AbstractPlan.AbstractJoinPlan.HashJoinPlan
|  |    join_type       : JoinType              # ok
|  |    predicate       : AbstractExpression    # ok
|  |    proj_info       : ProjectInfo
|  |                        tl  : TargetList    # need AbstractExpression
|  |                        dml : DirectMapList # ok
|  |    proj_schema     : Schema                # ok
|  |    left_hash_keys  : [AbstractExpression]  # ok
|  |    right_hash_keys : [AbstractExpression]  # ok
|  |
|  +- AbstractPlan.AbstractJoinPlan.(Unsupported Plans)
| 
+- AbstractPlan.(Unsupported Plans)
```
