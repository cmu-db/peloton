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
enum class RulePriority : int {
  HIGH = 3,
  MEDIUM = 2,
  LOW = 1
};

/*
 * Comparator Elimination: When two constant values are compared against
 *   each other (==, !=, >, <, >=, <=), the comparison expression gets rewritten
 *   to either TRUE or FALSE, depending on whether the constants agree with the
 *   comparison or not
 * Examples:
 *   "1 == 2" ==> "FALSE"
 *   "3 <= 4" ==> "TRUE"
 */
class ComparatorElimination: public Rule {
 public:
  ComparatorElimination(RuleType rule, ExpressionType root);

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Equivalent Transform: When a symmetric operator (==, !=, AND, OR) has two
 *   children, the comparison expression gets its arguments flipped.
 * Examples:
 *   "T.X != 3" ==> "3 != T.X"
 *   "(T.X == 1) AND (T.Y == 2)" ==> "(T.Y == 2) AND (T.X == 1)"
 */
class EquivalentTransform: public Rule {
 public:
  EquivalentTransform(RuleType rule, ExpressionType root);

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Tuple Value Equality with Two Constant Values: When the same tuple reference
 *   is checked against two distinct constant values, the expression is rewritten
 *   to FALSE
 * Example:
 *   "(T.X == 3) AND (T.X == 4)" ==> "FALSE"
 */
class TVEqualityWithTwoCVTransform: public Rule {
 public:
  TVEqualityWithTwoCVTransform();

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Transitive Closure w/ Constants: When two tuple references are compared against each
 *   other and one of the tuple references is compared to a constant, the expression
 *   swaps out the doubled tuple reference for the constant.
 * Example:
 *   "(T.X == Q.Y) AND (T.X == 6)" ==> "(6 == Q.Y) AND (T.X == 6)"
 */
class TransitiveClosureConstantTransform: public Rule {
 public:
  TransitiveClosureConstantTransform();

  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * And Short Circuiting: Anything AND FALSE is rewritten to FALSE.
 */
class AndShortCircuit: public Rule {
 public:
  AndShortCircuit();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Or Short Circuiting: Anything OR TRUE is rewritten to TRUE.
 */
class OrShortCircuit: public Rule {
 public:
  OrShortCircuit();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Null Lookup on Not Null Column: Asking if a tuple reference is NULL is rewritten
 *   to FALSE only when the catalog says that that attribute has a non-NULL constraint.
 * Example:
 *   "T.X IS NULL" ==> "FALSE" (assuming T.X is a non-NULL attribute)
 */
class NullLookupOnNotNullColumn: public Rule {
 public:
  NullLookupOnNotNullColumn();

  int Promise(GroupExprTemplate *group_expr, OptimizeContext *context) const override;
  bool Check(std::shared_ptr<AbstractNodeExpression> plan, OptimizeContext *context) const override;
  void Transform(std::shared_ptr<AbstractNodeExpression> input,
                 std::vector<std::shared_ptr<AbstractNodeExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/*
 * Not Null Lookup on Not Null Column: Asking if a tuple reference is NOT NULL is rewritten
 *   to TRUE only when the catalog says that that attribute has a non-NULL constraint.
 * Example:
 *   "T.X IS NOT NULL" ==> "TRUE" (assuming T.X is a non-NULL attribute)
 */
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
