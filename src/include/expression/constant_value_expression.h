//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_value_expression.h
//
// Identification: src/include/expression/constant_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "util/hash_util.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ConstantValueExpression
// Represents a constant value like int and string.
//===----------------------------------------------------------------------===//

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const type::Value &value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.GetTypeId()),
        value_(value.Copy()) {}

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    return value_;
  }

  virtual void DeduceExpressionName() override {
    if (!alias.empty()) return;
    expr_name_ = value_.ToString();
  }

  virtual bool Equals(AbstractExpression *expr) const override {
    if (exp_type_ != expr->GetExpressionType()) return false;
    auto const_expr = (ConstantValueExpression *)expr;
    return value_.CompareEquals(const_expr->value_);
  }

  type::Value GetValue() const { return value_; }

  bool HasParameter() const override { return false; }

  AbstractExpression *Copy() const override {
    return new ConstantValueExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  virtual hash_t Hash() const override {
    hash_t hash = HashUtil::Hash(&exp_type_);
    return HashUtil::CombineHashes(hash, value_.Hash());
  }

  bool IsNullable() const override { return false; }

 protected:
  ConstantValueExpression(const ConstantValueExpression &other)
      : AbstractExpression(other), value_(other.value_) {}

  type::Value value_;
};

}  // namespace expression
}  // namespace peloton
