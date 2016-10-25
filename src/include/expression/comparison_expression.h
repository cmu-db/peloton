//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_expression.h
//
// Identification: src/include/expression/comparison_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ComparisonExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class ComparisonExpression : public AbstractExpression {
 public:
  ~ComparisonExpression() {}
  
  ComparisonExpression(ExpressionType type)
    : AbstractExpression(type, Type::BOOLEAN) {}

  ComparisonExpression(ExpressionType type,
                       AbstractExpression *left,
                       AbstractExpression *right)
    : AbstractExpression(type, Type::BOOLEAN, left, right) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
        return vl.CompareEquals(vr);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
        return vl.CompareNotEquals(vr);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
        return vl.CompareLessThan(vr);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
        return vl.CompareGreaterThan(vr);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
        return vl.CompareLessThanEquals(vr);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
        return vl.CompareGreaterThanEquals(vr);
      default:
        throw Exception("Invalid comparison expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ComparisonExpression(exp_type_,
                                    left_ ? left_->Copy() : nullptr,
                                    right_ ? right_->Copy() : nullptr);
  }
};

}  // End expression namespace
}  // End peloton namespace
