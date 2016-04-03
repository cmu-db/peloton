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

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

// Returns a null value if the two specified expressions are equal.
// Otherwise, returns the first expression result.
class NullIfExpression : public AbstractExpression {
 public:
  NullIfExpression(ValueType vt,
                   const std::vector<AbstractExpression *> &expressions)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NULLIF),
        expressions(expressions),
        value_type(vt) {}

  virtual ~NullIfExpression() {
    for (auto value : expressions) delete value;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(expressions.size() == 2);

    auto left_result = expressions[0]->Evaluate(tuple1, tuple2, context);
    auto right_result = expressions[1]->Evaluate(tuple1, tuple2, context);

    if (left_result == right_result) {
      return Value::GetNullValue(value_type);
    } else {
      // FIXME: Order of expressions got reversed somewhere
      return right_result;
    }
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "NullIfExpression";
  }

    AbstractExpression *Copy() const {
      std::vector<AbstractExpression *> copied_expression;
      for (AbstractExpression *expression : expressions) {
        if (expression == nullptr) {
          continue;
        }
        copied_expression.push_back(expression->Copy());
      }

      return new NullIfExpression(value_type, copied_expression);
    }

 private:
  // Specified expressions
  std::vector<AbstractExpression *> expressions;

  ValueType value_type;
};

}  // End expression namespace
}  // End peloton namespace
