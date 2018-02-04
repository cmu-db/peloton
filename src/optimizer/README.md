## Optimizer Overview

The optimizer follows the [Cascade](http://15721.courses.cs.cmu.edu/spring2018/papers/15-optimizer1/graefe-ieee1995.pdf) framework, it takes an annotated parse tree as input, goes through multiple optimization phases, and output an execution plan. The major phases are:
* Parse tree to logical operator tree transformation.
* Predicate push-down, which pushes predicates to the lowest possible operator to evaluate.
* Unnesting, which turns arbitary correlated subqeries into logical join operator.
* Logical transformation, which enumerates all possible join orders.
* Stats derivation, which derives stats needed to compute the cost for each group.
* Phyisical implementation, which enumerates all possible implementation for a logical operator and cost them, e.g. hash join v.s. nested-loop join
* Property enforcing, which adds missing properties descirbing the output format, e.g. sort order.
* Operator to plan transformation, which turns the best physical operator tree into an execution plan.

The rewrite phase consists of predicate push-down and unnesting, they will run once separately before later optimization phases. Transformation, stats derivation, physical implementation and property enforcing will not be separated, a new logical operator tree generated from a transformation rule is going to be implemented as a physical operator and cost immediately, this allows us to do pruning based on the cost to avoid enumerating inefficient plans. Consider we are calculating cost for an operator tree `((A join B join C) join D)`. If the cost of only joining the intermidiate table `(A join B join C)` and the base table `D` already exceeded the cost of another operator tree in the same group, say, `((A join B) join (C join D))`, we'll do the pruning by avoiding join order enumeration for `(A join B join C)`. If we do transformation and implementation separately, the pruning is not possible, because when we're costing an operator tree after the implementation phase, the join order of all it's child groups have already been enumerated in the transformation phase.

The entrance of the optimizer is `Optimizer::BuildPelotonPlanTree()` in [`optimizer.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/optimizer.cpp). We'll cover each phase in the following sections.

## Parse Tree Transformation

The first step in the optimizer is to transform a peloton parse tree into a logical operator tree which follows relational algreba. This is implemented in [`query_to_operator_transformer.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/query_to_operator_transformer.cpp). Most of the transformations are trivial since the parse tree is mostly structurally similar to a relational algreba tree. There are two things worth mentioning:
* First, we extract conjunction expressions into a vector of expressions, annotate them with the table aliases occurred in the expression, and uses a separate predicate operators to store them. This will allow us to implement rule-based predicate push-down much easier. 
* Second, we transform correlated subqueries into `dependent join` and `mark join` intrudoced in [this paper](http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf) from Hyper, in the unnesting phase we'll transform dependent join into regular join operators.

## Rewrite Phase

The rewrite phase includes heuristic-based optimization passes that will be run once. For extensibility, we implement these passes as rule-based optimization. These passes assume the transformed tree is always better than the original tree so when a rule is applied, the new pattern will always replace the old one. 

We implemented two different rewrite frameworks, top-down rewrite and bottom-up rewrite in [`optimizer_task.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/optimizer_task.cpp). Both of them are designed for a single optimization pass. Basically, a top-down pass will start from the root operator, apply a set of rewrite rules until saturated, then move to lower level to apply rules for those operators, whereas a bottom-up pass will first recursively apply rules for current operator's children, then apply rules for the current operator. If any rule triggers for an operator in a bottom-up pass, we'll apply rules for its child again, since the children may have changed so rewrite rules may be applicable to them.

The difference between top-down and bottom-up rewrite is top-down rewrite may have less ability of expression, but usually is more efficient. Rewrite phases that could be done in a top-down pass probably could also be implemented using bottom-up framework. For optimizations with top-down nature, e.g. predicate push-down, a top-down pass usually will be more efficient since it only applies a set of rules for each operator once, but bottom-up framework may apply for each operator multiple time because when a rule is applied for an operator, we need to recurisively apply rules for all its children.

For people want to add new rewrite passes, they should specify a set of rules, pick a rewrite framework from top-down rewrite and bottom-up rewrite and push the rewrite task to the task queue.

Predicate push-down is a top-down rewrite pass, what it does is to push predicate throgh operators and when the predicate cannot be further push-down, the predicate is combined with the underlying operator.

Unnesting is a bottom-up pass that eliminates dependent join. It uses a bunch of techniques mentioned in Patrick's report.

## Cascade Style Optimization

After the rewrite phase we'll get a logical operator tree, the next step is to feed the logical operator tree into a Cascade style query optimizer to generate the lowest cost operator tree. The implementation basically follows the [Columbia Optimizer paper](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.54.1153&rep=rep1&type=pdf), we'll add more details in the documentation, for now please just take the paper as reference. The tasks are implemented in [`optimizer_task.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/optimizer_task.cpp). 

There's one task `DeriveStats` that is not mentioned in the Columnbia paper. We follow the [Orca paper](http://15721.courses.cs.cmu.edu/spring2018/papers/15-optimizer1/p337-soliman.pdf) to derive stats for each group on the fly. When a new group is generated, we'll recursively collect stats for the column used in the root operator's predicate (When a new group is generated, there's only one expression in the group, thus only one root operator), compute stats for the root group and cache those stats in the group so that we only derive stats for columns we need, which is efficient for both time and space.

The design of property enforcing also follows Orca rather than Columbia. We'll add enforcers after applying physical rules.

## Operator to plan transformation

When all the optimizations are done, we'll pick the lowest cost operator tree and use it to generate an execution plan.

We'll first derive the output column, which are a vector of expressions, for the root group based on the query's select list, then we'll recursively derive output columns for each operator (implemented in [`input_column_deriver.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/input_column_deriver.cpp)) based on columns used in each operator. At last, we generate peloton plan node using operators in the best operator tree and the input/output column pairs for these operators. What we do here is basically setting column offset for the plan nodes, e.g. output column offset, sort column's offset and column offset in the predicates (implemented in [`plan_generator.cpp`](https://github.com/chenboy/peloton/blob/optimizer_doc/src/optimizer/plan_generator.cpp)).

## WIP

There are still a lot of interesting work needed to be implemented, including:
* Join order enumeration. 
* Expression rewrite, my current thought is it should be done in the binder after annotating expressions or in the optimizer before predicate push-down.
* Implement sampling-based stats derivation and cost calculation
* Support unnesting arbitary queries so that we can support a wider range of queries in TPC-H. This would need the codegen engine to support `semi join`, `anti-semi join`, `mark join`, `single join`.
* More documentations.
