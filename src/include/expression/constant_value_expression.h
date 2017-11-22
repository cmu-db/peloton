//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_value_expression.h
//
// Identification: src/include/expression/constant_value_expression.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
  
  virtual bool ExactlyEquals(const AbstractExpression &other) const override {
    if (exp_type_ != other.GetExpressionType())
      return false;
    auto const_expr = static_cast<const ConstantValueExpression &>(other);
    return value_.CompareEquals(const_expr.value_);
  }

  virtual hash_t HashForExactMatch() const override {
    hash_t hash = HashUtil::Hash(&exp_type_);
    return HashUtil::CombineHashes(hash, value_.Hash());
  }

  virtual bool operator==(const AbstractExpression &rhs) const override {
    auto type = rhs.GetExpressionType();
    if (type != ExpressionType::VALUE_PARAMETER && exp_type_ != type)
      return false;
    if (return_value_type_ != rhs.GetValueType())
      return false;
    // Do not check value since we are going to parameterize and cache
    return true;
  }

  virtual bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  virtual hash_t Hash() const override {
    // Use VALUE_PARAMTER for parameterization with the compiled query cache
    auto val = ExpressionType::VALUE_PARAMETER;
    hash_t hash = HashUtil::Hash(&val);
    // Do not hash value since we are going to parameterize and cache
    return HashUtil::CombineHashes(hash, HashUtil::Hash(&return_value_type_));
  }

  virtual void VisitParameters(std::vector<Parameter> &parameters,
      std::unordered_map<const AbstractExpression *, size_t> &index,
      UNUSED_ATTRIBUTE const std::vector<peloton::type::Value>
          &parameter_values) override {
    // Add a new parameter object for a constant
    parameters.push_back(Parameter::CreateConstParameter(GetValue(),
                                                         IsNullable()));
    index[this] = parameters.size() - 1;
  };

  type::Value GetValue() const { return value_; }

  bool HasParameter() const override { return false; }

  AbstractExpression *Copy() const override {
    return new ConstantValueExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  bool IsNullable() const override { return value_.IsNull(); }

 protected:
  ConstantValueExpression(const ConstantValueExpression &other)
      : AbstractExpression(other), value_(other.value_) {}

  type::Value value_;
};

}  // namespace expression
}  // namespace peloton
