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
class InnerJoinCommutativity : public Rule {
 public:
  InnerJoinCommutativity();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

//===--------------------------------------------------------------------===//
// Implementation rules
//===--------------------------------------------------------------------===//

/**
 * @brief (Logical Scan -> Sequential Scan)
 */
class GetToSeqScan : public Rule {
 public:
  GetToSeqScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief Generate dummy scan for queries like "SELECT 1", there's no actual
 * table to generate
 */
class GetToDummyScan : public Rule {
 public:
  GetToDummyScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Scan -> Index Scan)
 */
class GetToIndexScan : public Rule {
 public:
  GetToIndexScan();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief Transforming query derived scan for nested query
 */
class LogicalQueryDerivedGetToPhysical : public Rule {
 public:
  LogicalQueryDerivedGetToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Delete -> Physical Delete)
 */
class LogicalDeleteToPhysical : public Rule {
 public:
  LogicalDeleteToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Update -> Physical Update)
 */
class LogicalUpdateToPhysical : public Rule {
 public:
  LogicalUpdateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Insert -> Physical Insert)
 */
class LogicalInsertToPhysical : public Rule {
 public:
  LogicalInsertToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Insert Select -> Physical Insert Select)
 */
class LogicalInsertSelectToPhysical : public Rule {
 public:
  LogicalInsertSelectToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Group by -> Hash Group by)
 */
class LogicalGroupByToHashGroupBy : public Rule {
 public:
  LogicalGroupByToHashGroupBy();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Aggregate -> Physical Aggregate)
 */
class LogicalAggregateToPhysical : public Rule {
 public:
  LogicalAggregateToPhysical();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Inner Join -> Inner Nested-Loop Join)
 */
class InnerJoinToInnerNLJoin : public Rule {
 public:
  InnerJoinToInnerNLJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Inner Join -> Inner Hash Join)
 */
class InnerJoinToInnerHashJoin : public Rule {
 public:
  InnerJoinToInnerHashJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Distinct -> Physical Distinct)
 */
class ImplementDistinct : public Rule {
 public:
  ImplementDistinct();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief (Logical Limit -> Physical Limit)
 */
class ImplementLimit : public Rule {
 public:
  ImplementLimit();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
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
class PushFilterThroughJoin : public Rule {
 public:
  PushFilterThroughJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief Combine multiple filters into one single filter using conjunction
 */
class CombineConsecutiveFilter : public Rule {
 public:
  CombineConsecutiveFilter();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * @brief Embed a filter into a scan operator. After predicate push-down, we
 * eliminate all filters in the operator trees, predicates should be associated
 * with get or join
 */
class EmbedFilterIntoGet : public Rule {
 public:
  EmbedFilterIntoGet();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// MarkJoinGetToInnerJoin
class MarkJoinGetToInnerJoin : public Rule {
 public:
  MarkJoinGetToInnerJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// MarkJoinInnerJoinToInnerJoin
class MarkJoinInnerJoinToInnerJoin : public Rule {
 public:
  MarkJoinInnerJoinToInnerJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughMarkJoin
class PullFilterThroughMarkJoin : public Rule {
 public:
  PullFilterThroughMarkJoin();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

}  // namespace optimizer
}  // namespace peloton
