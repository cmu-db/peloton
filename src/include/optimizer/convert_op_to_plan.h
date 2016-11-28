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

#include "expression/abstract_expression.h"
#include "optimizer/op_expression.h"
#include "planner/abstract_plan.h"

#include <memory>

namespace peloton {
namespace optimizer {

planner::AbstractPlan *ConvertOpExpressionToPlan(
    std::shared_ptr<OpExpression> plan);

} /* namespace optimizer */
} /* namespace peloton */
