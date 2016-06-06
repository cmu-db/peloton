//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// coalesce_expression.h
//
// Identification: src/include/expression/coalesce_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include <memory>
#include <vector>

#include "expression/expression_util.h"

namespace peloton {
namespace expression {

// Evaluates the arguments in order and returns the current value of the first
// expression that initially does not evaluate to NULL.
class CoalesceExpression : public AbstractExpression {
 public:
  typedef std::unique_ptr<AbstractExpression> AbstractExprPtr;
  CoalesceExpression(ValueType vt, std::vector<AbstractExprPtr> &expressions)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_COALESCE, vt),
        expressions_(std::move(expressions)) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    for (auto &expression : expressions_) {
      auto result = expression->Evaluate(tuple1, tuple2, context);
      if (result.IsNull()) continue;
      return result;
    }

    return Value::GetNullValue(GetValueType());
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return spacer + "CoalesceExpression";
  }

  AbstractExpression *Copy() const override {
    std::vector<AbstractExprPtr> copied_expression;
    for (auto &expression : expressions_) {
      if (expression == nullptr) {
        continue;
      }
      copied_expression.push_back(AbstractExprPtr(expression->Copy()));
    }

    return new CoalesceExpression(GetValueType(), copied_expression);
  }

 private:
  // Expression arguments
  std::vector<AbstractExprPtr> expressions_;
};

}  // End expression namespace
}  // End peloton namespace
