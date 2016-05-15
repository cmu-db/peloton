//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_value_expression.cpp
//
// Identification: src/backend/expression/parameter_value_expression.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/expression/parameter_value_expression.h"
#include "backend/executor/executor_context.h"

namespace peloton {
namespace expression {

ParameterValueExpression::ParameterValueExpression(ValueType type,
                                                   int value_idx)
    : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER, type),
      value_idx_(value_idx),
      param_value_() {
  LOG_TRACE("ParameterValueExpression %d", value_idx);
};

ParameterValueExpression::ParameterValueExpression(oid_t value_idx,
                                                   Value param_value)
    : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER,
                         param_value.GetValueType()),
      value_idx_(value_idx),
      param_value_(param_value) {}

Value ParameterValueExpression::Evaluate(
    UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
    UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
    executor::ExecutorContext *context) const {
  auto& params = context->GetParams();
  ALWAYS_ASSERT(value_idx_ < params.size());
  return params[value_idx_];
}

}  // namespace expression
}  // namespace peloton
