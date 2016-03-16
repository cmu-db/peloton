//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vector_expression.h
//
// Identification: src/backend/expression/case_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

// Evaluates the arguments in order and returns the current value of the first
// expression that initially does not evaluate to NULL.
class CoalesceExpression : public AbstractExpression {

 public:
  CoalesceExpression(ValueType vt, const std::vector<AbstractExpression *>& expressions)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NULLIF),
        expressions(expressions),
        value_type(vt) {}

  virtual ~CoalesceExpression() {
    for (auto value : expressions)
      delete value;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto value : expressions) {
      auto result = value->Evaluate(tuple1, tuple2, context);
      if (result.IsNull()) continue;
      return result;
    }

    return Value::GetNullValue(value_type);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CoalesceExpression";
  }

 private:
  // Expression arguments
  std::vector<AbstractExpression *> expressions;

  ValueType value_type;

};

}  // End expression namespace
}  // End peloton namespace
