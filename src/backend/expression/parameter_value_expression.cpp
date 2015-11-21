//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// parameter_value_expression.cpp
//
// Identification: src/backend/expression/parameter_value_expression.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/common/value_vector.h"
#include "backend/expression/parameter_value_expression.h"
#include "backend/executor/executor_context.h"

namespace peloton {
namespace expression {

    ParameterValueExpression::ParameterValueExpression(int value_idx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER),
        m_valueIdx(value_idx), m_paramValue()
    {
        LOG_TRACE("ParameterValueExpression %d", value_idx);
    };

    Value ParameterValueExpression::Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                                            __attribute__((unused)) const AbstractTuple *tuple2,
                                            executor::ExecutorContext *context) const{
      auto params = context->GetParams();
      assert(m_valueIdx < params.GetSize());

      return params[m_valueIdx];
    }


}  // End expression namespace
}  // End peloton namespace

