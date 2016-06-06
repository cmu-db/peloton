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

#include "common/value.h"

#include "expression/abstract_expression.h"

#include <vector>
#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class ParameterValueExpression : public AbstractExpression {
 public:
  // Constructor to initialize the PVE from the static parameter vector
  // from the VoltDBEngine instance. After the construction the PVE points
  // to the Value from the global vector.
  ParameterValueExpression(ValueType type, int value_idx);

  // Constructor to use for testing purposes
  ParameterValueExpression(oid_t value_idx, Value paramValue);

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override;

  bool HasParameter() const {
    // this class represents a parameter.
    return true;
  }

  std::string DebugInfo(const std::string &spacer) const override {
    std::ostringstream buffer;
    buffer << spacer << "OptimizedParameter[" << this->value_idx_ << "]\n";
    return buffer.str();
  }

  int GetParameterId() const { return this->value_idx_; }

  AbstractExpression *Copy() const override {
    return new ParameterValueExpression(value_idx_, param_value_);
  }

 private:
  oid_t value_idx_;
  Value param_value_;
};

}  // End expression namespace
}  // End peloton namespace
