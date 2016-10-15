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

  std::unique_ptr<Value> Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (exp_type_ == EXPRESSION_TYPE_OPERATOR_NOT) {
      auto vl = left_->Evaluate(tuple1, tuple2, context);
      if (vl->IsTrue())
        return std::unique_ptr<Value>(new Value(Type::BOOLEAN, 0));
      else if (vl->IsFalse())
        return std::unique_ptr<Value>(new Value(Type::BOOLEAN, 1));
      else
        return std::unique_ptr<Value>(new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL));
    }
    auto eval1 = left_->Evaluate(tuple1, tuple2, context);
    auto vl = std::unique_ptr<Value>(eval1.get());
    eval1.release();
    auto eval2 = right_->Evaluate(tuple1, tuple2, context);
    auto vr = std::unique_ptr<Value>(eval2.get());
    eval2.release();
    switch (exp_type_) {
      case (EXPRESSION_TYPE_OPERATOR_PLUS):
        return std::unique_ptr<Value>(vl->Add(*vr));
      case (EXPRESSION_TYPE_OPERATOR_MINUS):
        return std::unique_ptr<Value>(vl->Subtract(*vr));
      case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
        return std::unique_ptr<Value>(vl->Multiply(*vr));
      case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
        return std::unique_ptr<Value>(vl->Divide(*vr));
      case (EXPRESSION_TYPE_OPERATOR_MOD):
        return std::unique_ptr<Value>(vl->Modulo(*vr));
      default:
        throw Exception("Invalid operator expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new OperatorExpression(exp_type_, value_type_,
                                  left_ ? left_->Copy() : nullptr,
                                  right_ ? right_->Copy() :nullptr);
  }
};

class OperatorUnaryMinusExpression : public AbstractExpression {
 public:
  OperatorUnaryMinusExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS,
                           left->GetValueType(), left, nullptr) {}

  std::unique_ptr<Value> Evaluate(const AbstractTuple *tuple1,
                                  const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    std::unique_ptr<Value> zero(new Value(Type::INTEGER, 0));
    return std::unique_ptr<Value>(zero->Subtract(*vl));
  }

  AbstractExpression *Copy() const override {
    return new OperatorUnaryMinusExpression(left_->Copy());
  }
};

}  // End expression namespace
}  // End peloton namespace
