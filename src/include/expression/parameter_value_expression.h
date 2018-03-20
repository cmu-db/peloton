//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_value_expression.h
//
// Identification: src/include/expression/parameter_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "executor/executor_context.h"
#include "common/sql_node_visitor.h"
#include "util/hash_util.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ParameterValueExpression
//===----------------------------------------------------------------------===//

class ParameterValueExpression : public AbstractExpression {
 public:
  ParameterValueExpression(int value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER,
                           type::TypeId::PARAMETER_OFFSET),
        value_idx_(value_idx),
        is_nullable_(false) {}

  int GetValueIdx() const { return value_idx_; }

  type::Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                       UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override {
    return context->GetParamValues().at(value_idx_);
  }

  AbstractExpression *Copy() const override {
    return new ParameterValueExpression(value_idx_);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  virtual bool operator==(const AbstractExpression &rhs) const override {
    auto type = rhs.GetExpressionType();
    if ((type != ExpressionType::VALUE_CONSTANT) &&
        (exp_type_ != type || return_value_type_ != rhs.GetValueType())) {
      return false;
    }

    auto &other =
        static_cast<const expression::ParameterValueExpression &>(rhs);
    // Do not check value since we are going to parameterize and cache
    // but, check the nullability for optimizing the non-nullable cases
    if (is_nullable_ != other.IsNullable()) {
      return false;
    }

    return true;
  }

  virtual bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  virtual hash_t Hash() const override {
    hash_t hash = HashUtil::Hash(&exp_type_);

    // Do not hash value since we are going to parameterize and cache
    // but, check the nullability for optimizing the non-nullable cases
    return HashUtil::CombineHashes(hash, HashUtil::Hash(&is_nullable_));
  }

  bool IsNullable() const override { return is_nullable_; }

  virtual void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override {
    // Add a new parameter object for a parameter
    auto &value = values_from_user[value_idx_];

    // We update nullability from the value and keep this in expression
    is_nullable_ = value.IsNull();
    map.Insert(Parameter::CreateParamParameter(value.GetTypeId(), is_nullable_),
               this);
    values.push_back(value);
    return_value_type_ = value.GetTypeId();
  };

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 protected:
  // Index of the value in the value vectors coming in from the user
  int value_idx_;

  // Nullability, updated after combining with value from the user
  bool is_nullable_;

  ParameterValueExpression(const ParameterValueExpression &other)
      : AbstractExpression(other), value_idx_(other.value_idx_) {}
};

}  // namespace expression
}  // namespace peloton
