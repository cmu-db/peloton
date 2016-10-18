//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/expression/conjunction_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class ConjunctionExpression : public AbstractExpression {
 public:
  ConjunctionExpression(ExpressionType type)
    : AbstractExpression(type) {}

  ConjunctionExpression(ExpressionType type,
                        AbstractExpression *left,
                        AbstractExpression *right)
    : AbstractExpression(type, Type::BOOLEAN, left, right) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case (EXPRESSION_TYPE_CONJUNCTION_AND): {
        if (vl.IsTrue() && vr.IsTrue())
          return ValueFactory::GetBooleanValue(1);
        if (vl.IsFalse() || vr.IsFalse())
          return ValueFactory::GetBooleanValue(0);
        return ValueFactory::GetBooleanValue(
            PELOTON_BOOLEAN_NULL);
      }
      case (EXPRESSION_TYPE_CONJUNCTION_OR): {
        if (vl.IsFalse() && vr.IsFalse())
          return ValueFactory::GetBooleanValue(0);
        if (vl.IsTrue() || vr.IsTrue())
          return ValueFactory::GetBooleanValue(1);
        return ValueFactory::GetBooleanValue(
            PELOTON_BOOLEAN_NULL);
      }
      default:
        throw Exception("Invalid conjunction expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ConjunctionExpression(exp_type_,
                                     left_ ? left_->Copy() : nullptr,
                                     right_ ? right_->Copy() : nullptr);
  }
};

}  // End expression namespace
}  // End peloton namespace
