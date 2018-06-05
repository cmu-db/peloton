//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_query_rewrite.h
//
// Identification: src/include/optimizer/rule_query_rewrite.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"

#include <memory>

namespace peloton {
namespace optimizer {

/**
 * Implements transitive predicates generation for
 * LogicalGet nodes:
 *   a = b AND b = c -> a = b AND b = c AND a = c
 */
class TransitivePredicatesLogicalGet : public Rule {
 public:
  TransitivePredicatesLogicalGet();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * Implements transitive predicates generation for
 * LogicalFilter nodes:
 *   a = b AND b = c -> a = b AND b = c AND a = c
 */
class TransitivePredicatesLogicalFilter : public Rule {
 public:
  TransitivePredicatesLogicalFilter();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

/**
 * Implements predicate simplification for
 * LogicalFilter nodes:
 *   a = a AND b = c -> b = c
 */
class SimplifyPredicatesLogicalFilter : public Rule {
 public:
  SimplifyPredicatesLogicalFilter();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};
}  // namespace optimizer
}  // namespace peloton
