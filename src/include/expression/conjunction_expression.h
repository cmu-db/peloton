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
  ConjunctionExpression(ExpressionType type) : AbstractExpression(type) {}

  ConjunctionExpression(ExpressionType type, AbstractExpression *left,
                        AbstractExpression *right)
      : AbstractExpression(type, Type::BOOLEAN, left, right) {}

  Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    PL_ASSERT(children_.size() == 2);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    auto vr = children_[1]->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case (EXPRESSION_TYPE_CONJUNCTION_AND): {
        if (vl.IsTrue() && vr.IsTrue())
          return ValueFactory::GetBooleanValue(true);
        if (vl.IsFalse() || vr.IsFalse())
          return ValueFactory::GetBooleanValue(false);
        return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
      }
      case (EXPRESSION_TYPE_CONJUNCTION_OR): {
        if (vl.IsFalse() && vr.IsFalse())
          return ValueFactory::GetBooleanValue(false);
        if (vl.IsTrue() || vr.IsTrue())
          return ValueFactory::GetBooleanValue(true);
        return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
      }
      default:
        throw Exception("Invalid conjunction expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ConjunctionExpression(*this);
  }

 protected:
  ConjunctionExpression(const ConjunctionExpression &other)
      : AbstractExpression(other) {}
};

}  // End expression namespace
}  // End peloton namespace
