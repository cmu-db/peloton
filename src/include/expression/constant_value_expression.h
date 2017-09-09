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
#include "type/value.h"
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
    if (!alias.empty())
      return;
    expr_name_ = value_.ToString();
  }
  
  virtual bool operator==(const AbstractExpression &rhs) const override {
    if (exp_type_ != rhs.GetExpressionType())
      return false;
    auto &other = static_cast<const ConstantValueExpression &>(rhs);
    return value_.CompareEquals(other.value_);
  }

  virtual bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
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
