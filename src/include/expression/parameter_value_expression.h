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
        value_idx_(value_idx) {}

  int GetValueIdx() const { return value_idx_; }

  type::Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                       UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override {
    return context->GetParams().at(value_idx_);
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
    return true;
  }

  virtual bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  virtual hash_t Hash() const override {
    return HashUtil::Hash(&exp_type_);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override {
    // Add a new parameter object for a parameter
    auto &value = values_from_user[value_idx_];
    auto is_nullable = true;
    map.Insert(Parameter::CreateParamParameter(value.GetTypeId(), is_nullable),
               this);
    values.push_back(value);
    return_value_type_ = value.GetTypeId();
  };

 protected:
  int value_idx_;

  ParameterValueExpression(const ParameterValueExpression &other)
      : AbstractExpression(other), value_idx_(other.value_idx_) {}
};

}  // namespace expression
}  // namespace peloton
