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

  bool operator==(const AbstractExpression &rhs) const override {
    if (exp_type_ != rhs.GetExpressionType())
      return false;
    auto &other = static_cast<const ParameterValueExpression &>(rhs);
    if (value_idx_ != other.GetValueIdx())
      return false;
    return true;
  }

  bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  void ExtractParameters(std::vector<planner::Parameter> &parameters,
      std::unordered_map<const AbstractExpression *, size_t> &index) const
      override {
    AbstractExpression::ExtractParameters(parameters, index);

    // Add a new parameter object for a parameter
    parameters.push_back(planner::Parameter::CreateParameter(GetValueType(),
                                                             GetValueIdx()));
    index[this] = parameters.size() - 1;
  };

 protected:
  int value_idx_;

  ParameterValueExpression(const ParameterValueExpression &other)
      : AbstractExpression(other), value_idx_(other.value_idx_) {}
};

}  // namespace expression
}  // namespace peloton
