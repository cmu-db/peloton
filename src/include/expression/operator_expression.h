//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/expression/operator_expression.h
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

class OperatorExpression : public AbstractExpression {
 public:
  OperatorExpression(ExpressionType type, Type::TypeId type_id)
    : AbstractExpression(type, type_id) {}

  OperatorExpression(ExpressionType type,
                     Type::TypeId type_id,
                     AbstractExpression *left,
                     AbstractExpression *right)
    : AbstractExpression(type, type_id, left, right) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (exp_type_ == EXPRESSION_TYPE_OPERATOR_NOT) {
      PL_ASSERT(children_.size() == 1);
      Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
      if (vl.IsTrue())
        return (ValueFactory::GetBooleanValue(0));
      else if (vl.IsFalse())
        return (ValueFactory::GetBooleanValue(1));
      else
        return (ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL));
    }
    PL_ASSERT(children_.size() == 2);
    Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
    Value vr = children_[1]->Evaluate(tuple1, tuple2, context);

    switch (exp_type_) {
      case (EXPRESSION_TYPE_OPERATOR_PLUS):
        return (vl.Add(vr));
      case (EXPRESSION_TYPE_OPERATOR_MINUS):
        return (vl.Subtract(vr));
      case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
        return (vl.Multiply(vr));
      case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
        return (vl.Divide(vr));
      case (EXPRESSION_TYPE_OPERATOR_MOD):
        return (vl.Modulo(vr));
      default:
        throw Exception("Invalid operator expression type.");
    }
  }

  void DeduceExpressionType(){
    // if we are a decimal or int we should take the highest type id of both children
    // This relies on a particular order in types.h
    auto type = std::max(children_[0]->GetValueType(), children_[1]->GetValueType());
    PL_ASSERT(type <= Type::DECIMAL);
    return_value_type_ = type;
  }

  AbstractExpression *Copy() const override {
    return new OperatorExpression(*this);
  }

 protected:
  OperatorExpression(const OperatorExpression& other) :AbstractExpression(other){}
};

class OperatorUnaryMinusExpression : public AbstractExpression {
 public:
  OperatorUnaryMinusExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS,
                           left->GetValueType(), left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1,
                                  const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(children_.size() == 1);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    Value zero(ValueFactory::GetIntegerValue(0));
    return zero.Subtract(vl);
  }

  AbstractExpression *Copy() const override {
    return new OperatorUnaryMinusExpression(*this);
  }

 protected:
  OperatorUnaryMinusExpression(const OperatorUnaryMinusExpression &other)
        : AbstractExpression(other) {}
};

}  // End expression namespace
}  // End peloton namespace
