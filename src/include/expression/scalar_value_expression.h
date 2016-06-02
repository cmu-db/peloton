//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scalar_value_expression.h
//
// Identification: src/backend/expression/scalar_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

class ScalarValueExpression : public AbstractExpression {
 public:
  ScalarValueExpression(ValueType type, AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_VALUE_SCALAR, type, lc, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override;

  std::string DebugInfo(const std::string &spacer) const override;

  AbstractExpression *Copy() const override {
    return new ScalarValueExpression(GetValueType(), CopyUtil(GetLeft()));
  }
};

}  // End expression namespace
}  // End peloton namespace
