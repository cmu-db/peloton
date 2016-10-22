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

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ParameterValueExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class ParameterValueExpression : public AbstractExpression {
 public:
  ParameterValueExpression(int value_idx)
    : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER, Type::PARAMETER_OFFSET),
      value_idx_(value_idx){}

  int GetValueIdx() const { return value_idx_; }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      executor::ExecutorContext *context) const override {
    return context->GetParams().at(value_idx_);
  }

  AbstractExpression *Copy() const override {
    return new ParameterValueExpression(value_idx_);
  }

 protected:
  int value_idx_;
};

}  // End expression namespace
}  // End peloton namespace
