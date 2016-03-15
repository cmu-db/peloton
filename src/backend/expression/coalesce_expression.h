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

class CoalesceExpression : public AbstractExpression {
 private:
  // Arguments
  std::vector<AbstractExpression *> *values;
  ValueType valuetype;

 public:
  CoalesceExpression(ValueType vt, std::vector<AbstractExpression *> *values)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NULLIF),
        values(values),
        valuetype(vt) {}

  virtual ~CoalesceExpression() {
    for (auto value : *values) delete value;
    delete values;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto value : *values) {
      auto v = value->Evaluate(tuple1, tuple2, context);
      if (v.IsNull()) continue;
      return v;
    }
    return Value::GetNullValue(valuetype);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CoalesceExpression";
  }
};

}  // End expression namespace
}  // End peloton namespace
