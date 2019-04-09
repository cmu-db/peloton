//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_impls.h
//
// Identification: src/include/optimizer/rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"

#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Transformation rules
//===--------------------------------------------------------------------===//

/**
 * @brief (A join B) -> (B join A)
 */
class InnerJoinCommutativity : public Rule<Operator,OpType,OperatorExpression> {
 public:
  InnerJoinCommutativity();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (A join B) join C -> A join (B join C)
 */

class InnerJoinAssociativity : public Rule<Operator,OpType,OperatorExpression> {
 public:
  InnerJoinAssociativity();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

//===--------------------------------------------------------------------===//
// Implementation rules
//===--------------------------------------------------------------------===//

/**
 * @brief (Logical Scan -> Sequential Scan)
 */
class GetToSeqScan : public Rule<Operator,OpType,OperatorExpression> {
 public:
  GetToSeqScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

class LogicalExternalFileGetToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalExternalFileGetToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief Generate dummy scan for queries like "SELECT 1", there's no actual
 * table to generate
 */
class GetToDummyScan : public Rule<Operator,OpType,OperatorExpression> {
 public:
  GetToDummyScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Scan -> Index Scan)
 */
class GetToIndexScan : public Rule<Operator,OpType,OperatorExpression> {
 public:
  GetToIndexScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief Transforming query derived scan for nested query
 */
class LogicalQueryDerivedGetToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalQueryDerivedGetToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Delete -> Physical Delete)
 */
class LogicalDeleteToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalDeleteToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Update -> Physical Update)
 */
class LogicalUpdateToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalUpdateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Insert -> Physical Insert)
 */
class LogicalInsertToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalInsertToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Insert Select -> Physical Insert Select)
 */
class LogicalInsertSelectToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalInsertSelectToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Group by -> Hash Group by)
 */
class LogicalGroupByToHashGroupBy : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalGroupByToHashGroupBy();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Aggregate -> Physical Aggregate)
 */
class LogicalAggregateToPhysical : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalAggregateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Inner Join -> Inner Nested-Loop Join)
 */
class InnerJoinToInnerNLJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  InnerJoinToInnerNLJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Inner Join -> Inner Hash Join)
 */
class InnerJoinToInnerHashJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  InnerJoinToInnerHashJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Distinct -> Physical Distinct)
 */
class ImplementDistinct : public Rule<Operator,OpType,OperatorExpression> {
 public:
  ImplementDistinct();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief (Logical Limit -> Physical Limit)
 */
class ImplementLimit : public Rule<Operator,OpType,OperatorExpression> {
 public:
  ImplementLimit();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief Logical Export to External File -> Physical Export to External file
 */
class LogicalExportToPhysicalExport : public Rule<Operator,OpType,OperatorExpression> {
 public:
  LogicalExportToPhysicalExport();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

//===--------------------------------------------------------------------===//
// Rewrite rules
//===--------------------------------------------------------------------===//

/**
 * @brief perform predicate push-down to push a filter through join. For
 *  example, For query "SELECT test.a, test.b FROM test, test1 WHERE test.a = 5"
 *  we could push "test.a=5" through the join to evaluate at the table scan
 *  level
 */
class PushFilterThroughJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  PushFilterThroughJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief Combine multiple filters into one single filter using conjunction
 */
class CombineConsecutiveFilter : public Rule<Operator,OpType,OperatorExpression> {
 public:
  CombineConsecutiveFilter();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

/**
 * @brief perform predicate push-down to push a filter through aggregation, also
 * will embed filter into aggregation operator if appropriate.
 */
class PushFilterThroughAggregation : public Rule<Operator,OpType,OperatorExpression> {
 public:
  PushFilterThroughAggregation();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};
/**
 * @brief Embed a filter into a scan operator. After predicate push-down, we
 * eliminate all filters in the operator trees, predicates should be associated
 * with get or join
 */
class EmbedFilterIntoGet : public Rule<Operator,OpType,OperatorExpression> {
 public:
  EmbedFilterIntoGet();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// Unnesting rules
// We use this promise to determine which rules should be applied first if
// multiple rules are applicable, we need to first pull filters up through mark-join
// then turn mark-join into a regular join operator
enum class UnnestPromise { Low = 1, High };
// TODO(boweic): MarkJoin and SingleJoin should not be transformed into inner
// join. Sometimes MarkJoin could be transformed into semi-join, but for now we
// do not have these operators in the llvm cogen engine. Once we have those, we
// should not use the following rules in the rewrite phase
///////////////////////////////////////////////////////////////////////////////
/// MarkJoinGetToInnerJoin
class MarkJoinToInnerJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  MarkJoinToInnerJoin();

  int Promise(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
              OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};
///////////////////////////////////////////////////////////////////////////////
/// SingleJoinToInnerJoin
class SingleJoinToInnerJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  SingleJoinToInnerJoin();

  int Promise(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
              OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughMarkJoin
class PullFilterThroughMarkJoin : public Rule<Operator,OpType,OperatorExpression> {
 public:
  PullFilterThroughMarkJoin();

  int Promise(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
              OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughAggregation
class PullFilterThroughAggregation : public Rule<Operator,OpType,OperatorExpression> {
 public:
  PullFilterThroughAggregation();

  int Promise(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
              OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext<Operator,OpType,OperatorExpression> *context) const override;
};
}  // namespace optimizer
}  // namespace peloton
