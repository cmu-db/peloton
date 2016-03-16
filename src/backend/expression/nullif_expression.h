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

class NullIfExpression : public AbstractExpression {
 private:
  // Arguments
  std::vector<AbstractExpression *> *values;
  ValueType valuetype;

 public:
  NullIfExpression(ValueType vt, std::vector<AbstractExpression *> *values)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NULLIF),
        values(values),
        valuetype(vt) {}

  virtual ~NullIfExpression() {
    for (auto value : *values) delete value;
    delete values;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    Value v[2];

    for (std::vector<AbstractExpression *>::size_type i = 0; i < values->size();
         i++)
      v[i] = values->at(i)->Evaluate(tuple1, tuple2, context);

    return v[0] == v[1] ? Value::GetNullValue(valuetype)
                        : v[1];  // the order is reversed somewhere
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "NullIfExpression";
  }
};

}  // End expression namespace
}  // End peloton namespace
