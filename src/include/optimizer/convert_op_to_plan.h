//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_op_to_plan.h
//
// Identification: src/include/optimizer/convert_op_to_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/op_expression.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan.h"

#include <memory>

namespace peloton {
namespace optimizer {

expression::AbstractExpression *ConvertOpExpressionToAbstractExpression(
    std::shared_ptr<OpExpression> op_expr, std::vector<Column *> left_columns,
    std::vector<Column *> right_columns);

planner::AbstractPlan *ConvertOpExpressionToPlan(
    std::shared_ptr<OpExpression> plan);

} /* namespace optimizer */
} /* namespace peloton */
