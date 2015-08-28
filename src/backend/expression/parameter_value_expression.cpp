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

#include "parameter_value_expression.h"
#include "common/debuglog.h"
#include "common/valuevector.h"
#include "common/executorcontext.hpp"

namespace voltdb {

    ParameterValueExpression::ParameterValueExpression(int value_idx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER),
        m_valueIdx(value_idx), m_paramValue()
    {
        VOLT_TRACE("ParameterValueExpression %d", value_idx);
        ExecutorContext* context = ExecutorContext::getExecutorContext();

        NValueArray* params = context->getParameterContainer();

        assert(value_idx < params->size());
        m_paramValue = &(*params)[value_idx];
    };

}

