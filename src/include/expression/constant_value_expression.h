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

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ConstantValueExpression
// Represents a constant value like int and string.
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const Value &value)
    : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT,
                         value.GetTypeId()), value_(value.Copy()) {}
  
  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    return value_;
  }

  Value GetValue() const { return value_;}

  bool HasParameter() const override { return false; }

  AbstractExpression *Copy() const override {
    return new ConstantValueExpression(value_);
  }

 protected:
  Value value_;
};

}  // End expression namespace
}  // End peloton namespace
