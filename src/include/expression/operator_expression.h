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

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/postgresparser.h"
#include "type/value_factory.h"
#include "type/value.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

class OperatorExpression : public AbstractExpression {
 public:
  OperatorExpression(ExpressionType type, type::TypeId type_id)
      : AbstractExpression(type, type_id) {}

  OperatorExpression(ExpressionType type, type::TypeId type_id,
                     AbstractExpression *left, AbstractExpression *right)
      : AbstractExpression(type, type_id, left, right) {}

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (exp_type_ == ExpressionType::OPERATOR_NOT) {
      PL_ASSERT(children_.size() == 1);
      type::Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
      if (vl.IsTrue())
        return (type::ValueFactory::GetBooleanValue(false));
      else if (vl.IsFalse())
        return (type::ValueFactory::GetBooleanValue(true));
      else
        return (
            type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_NULL));
    }
    if (exp_type_ == ExpressionType::OPERATOR_EXISTS) {
      PL_ASSERT(children_.size() == 1);
      type::Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
      if (vl.IsNull())
        return (type::ValueFactory::GetBooleanValue(false));
      else
        return (type::ValueFactory::GetBooleanValue(true));
    }
    if (exp_type_ == ExpressionType::OPERATOR_IS_NULL ||
        exp_type_ == ExpressionType::OPERATOR_IS_NOT_NULL) {
      PL_ASSERT(children_.size() == 1);
      type::Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
      if (exp_type_ == ExpressionType::OPERATOR_IS_NULL) {
        return type::ValueFactory::GetBooleanValue(vl.IsNull());
      } else if (exp_type_ == ExpressionType::OPERATOR_IS_NOT_NULL) {
        return type::ValueFactory::GetBooleanValue(!vl.IsNull());
      }
    }
    PL_ASSERT(children_.size() == 2);
    type::Value vl = children_[0]->Evaluate(tuple1, tuple2, context);
    type::Value vr = children_[1]->Evaluate(tuple1, tuple2, context);

    switch (exp_type_) {
      case (ExpressionType::OPERATOR_PLUS):
        return (vl.Add(vr));
      case (ExpressionType::OPERATOR_MINUS):
        return (vl.Subtract(vr));
      case (ExpressionType::OPERATOR_MULTIPLY):
        return (vl.Multiply(vr));
      case (ExpressionType::OPERATOR_DIVIDE):
        return (vl.Divide(vr));
      case (ExpressionType::OPERATOR_MOD):
        return (vl.Modulo(vr));
      default:
        throw Exception("Invalid operator expression type.");
    }
  }

  void DeduceExpressionType() override {
    // if we are a decimal or int we should take the highest type id of both
    // children
    // This relies on a particular order in types.h
    if (exp_type_ == ExpressionType::OPERATOR_NOT ||
        exp_type_ == ExpressionType::OPERATOR_IS_NULL ||
        exp_type_ == ExpressionType::OPERATOR_IS_NOT_NULL ||
        exp_type_ == ExpressionType::OPERATOR_EXISTS) {
      return_value_type_ = type::TypeId::BOOLEAN;
      return;
    }
    auto type =
        std::max(children_[0]->GetValueType(), children_[1]->GetValueType());
    PL_ASSERT(type <= type::TypeId::DECIMAL);
    return_value_type_ = type;
  }

  AbstractExpression *Copy() const override {
    return new OperatorExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 protected:
  OperatorExpression(const OperatorExpression &other)
      : AbstractExpression(other) {}
};

class OperatorUnaryMinusExpression : public AbstractExpression {
 public:
  OperatorUnaryMinusExpression(AbstractExpression *left)
      : AbstractExpression(ExpressionType::OPERATOR_UNARY_MINUS,
                           left->GetValueType(), left, nullptr) {}

  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override {
    PL_ASSERT(children_.size() == 1);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    type::Value zero(type::ValueFactory::GetIntegerValue(0));
    return zero.Subtract(vl);
  }

  AbstractExpression *Copy() const override {
    return new OperatorUnaryMinusExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 protected:
  OperatorUnaryMinusExpression(const OperatorUnaryMinusExpression &other)
      : AbstractExpression(other) {}
};

}  // namespace expression
}  // namespace peloton
