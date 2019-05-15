//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_rewrite.h
//
// Identification: src/include/optimizer/rule_rewrite.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"
#include "optimizer/absexpr_expression.h"

#include <memory>

namespace peloton {
namespace optimizer {

using GroupExprTemplate = GroupExpression;
using OptimizeContext = OptimizeContext;

/* Rules are applied from high to low priority */
#define HIGH_PRIORITY 3
#define MEDIUM_PRIORITY 2
#define LOW_PRIORITY 1

class ComparatorElimination: public Rule {
 public:
  ComparatorElimination(RuleType rule, ExpressionType root);

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class EquivalentTransform: public Rule {
 public:
  EquivalentTransform(RuleType rule, ExpressionType root);

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class TVEqualityWithTwoCVTransform: public Rule {
 public:
  TVEqualityWithTwoCVTransform();

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class TransitiveClosureConstantTransform: public Rule {
 public:
  TransitiveClosureConstantTransform();

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class AndShortCircuit: public Rule {
 public:
  AndShortCircuit();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class OrShortCircuit: public Rule {
 public:
  OrShortCircuit();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class NullLookupOnNotNullColumn: public Rule {
 public:
  NullLookupOnNotNullColumn();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class NotNullLookupOnNotNullColumn: public Rule {
 public:
  NotNullLookupOnNotNullColumn();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

}  // namespace optimizer
}  // namespace peloton
