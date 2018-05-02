//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transitive_predicates.h
//
// Identification: src/include/optimizer/transitive_predicates.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"

#include <memory>

namespace peloton {
namespace optimizer {

class TransitivePredicatesLogicalGet : public Rule {
 public:
  TransitivePredicatesLogicalGet();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};

class TransitivePredicatesLogicalFilter : public Rule {
 public:
  TransitivePredicatesLogicalFilter();

  bool Check(std::shared_ptr<OperatorExpression> plan,
             OptimizeContext *context) const override;

  void Transform(std::shared_ptr<OperatorExpression> input,
                 std::vector<std::shared_ptr<OperatorExpression>> &transformed,
                 OptimizeContext *context) const override;
};
}  // namespace optimizer
}  // namespace peloton
