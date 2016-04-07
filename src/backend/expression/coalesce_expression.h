//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// coalesce_expression.h
//
// Identification: src/backend/expression/coalesce_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include <memory>
#include <vector>
namespace peloton {
namespace expression {

// Evaluates the arguments in order and returns the current value of the first
// expression that initially does not evaluate to NULL.
class CoalesceExpression : public AbstractExpression {
 public:
  typedef std::unique_ptr<AbstractExpression> AbstractExprPtr;
  CoalesceExpression(ValueType vt, std::vector<AbstractExprPtr> &t_expressions)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_COALESCE), value_type(vt) {
    for (auto &expression : t_expressions) {
      expressions.push_back(std::move(expression));
    }
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto &expression : expressions) {
      auto result = expression->Evaluate(tuple1, tuple2, context);
      if (result.IsNull()) continue;
      return result;
    }

    return Value::GetNullValue(value_type);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CoalesceExpression";
  }

  AbstractExpression *Copy() const {
    std::vector<AbstractExprPtr> copied_expression;
    for (auto &expression : expressions) {
      if (expression == nullptr) {
        continue;
      }
      copied_expression.push_back(AbstractExprPtr(expression->Copy()));
    }

    return new CoalesceExpression(value_type, copied_expression);
  }

 private:
  // Expression arguments
  std::vector<AbstractExprPtr> expressions;

  ValueType value_type;
};

}  // End expression namespace
}  // End peloton namespace
