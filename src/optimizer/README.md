## Optimizer Overview

The optimizer follows the cascade framework, it takes an annotated parse tree as input, goes through multiple optimization phases, and output an execution plan. The major phases are:

* Parse tree to logical operator tree transformation, which turns a parse tree into a logical operator tree.
* Predicate push-down, which pushes predicates to the lowest possible operator to evaluate.
* Unnesting, which turns sub-queries with corelation into regular join operator.
* Logical transformation, which enumerates all possible join order.
* Stats derivation, which derives stats needed to compute cost for each group.
* Phyisical implementation, which enumerates all possible implementation for a logical operator, e.g. hash join v.s. nested-loop join
* Property enforcing, which adds missing properties descirbing the output data's format, e.g. sort order.
* Operator to plan transformation, which turns the best physical operator tree into an execution plan.

Predicate push-down and unnesting are rewrite phases, they will run independently.
