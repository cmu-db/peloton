//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nullif_expression.h
//
// Identification: src/backend/expression/nullif_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include <memory>
namespace peloton {
namespace expression {

// Returns a null value if the two specified expressions are equal.
// Otherwise, returns the first expression result.
class NullIfExpression : public AbstractExpression {
 public:
  typedef std::unique_ptr<AbstractExpression> AbstractExprPtr;

  NullIfExpression(ValueType vt, std::vector<AbstractExprPtr> &expressions)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NULLIF, vt),
        expressions_(std::move(expressions)) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    assert(expressions_.size() == 2);

    auto left_result = expressions_[0]->Evaluate(tuple1, tuple2, context);
    auto right_result = expressions_[1]->Evaluate(tuple1, tuple2, context);

    if (left_result == right_result) {
      return Value::GetNullValue(GetValueType());
    } else {
      return left_result;
    }
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return spacer + "NullIfExpression";
  }

  AbstractExpression *Copy() const override {
    std::vector<AbstractExprPtr> copied_expression;
    for (auto &expression : expressions_) {
      if (expression == nullptr) {
        continue;
      }
      copied_expression.push_back(AbstractExprPtr(expression->Copy()));
    }
    return new NullIfExpression(GetValueType(), copied_expression);
  }

 private:
  // Specified expressions
  std::vector<AbstractExprPtr> expressions_;
};

}  // End expression namespace
}  // End peloton namespace
