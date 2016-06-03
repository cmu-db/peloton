//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_op_to_plan.h
//
// Identification: src/backend/optimizer/convert_op_to_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/op_expression.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_plan.h"

#include <memory>

namespace peloton {
namespace optimizer {

expression::AbstractExpression *ConvertOpExpressionToAbstractExpression(
  std::shared_ptr<OpExpression> op_expr,
  std::vector<Column *> left_columns,
  std::vector<Column *> right_columns);

planner::AbstractPlan *ConvertOpExpressionToPlan(
  std::shared_ptr<OpExpression> plan);

} /* namespace optimizer */
} /* namespace peloton */
