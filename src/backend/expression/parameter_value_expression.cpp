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

namespace peloton {
namespace expression {

    ParameterValueExpression::ParameterValueExpression(int value_idx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER),
        m_valueIdx(value_idx), m_paramValue()
    {
        LOG_TRACE("ParameterValueExpression %d", value_idx);
    };

}  // End expression namespace
}  // End peloton namespace

