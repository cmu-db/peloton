//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_result.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_result.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
//#include "backend/planner/result_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Result
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres Result into a Peloton Node.
 *
 * @return Pointer to the constructed AbstractPlan.
 *
 */
const std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformResult(
    const ResultPlanState *result_state) {
  LOG_INFO("Handle Result");

  // get the underlying plan
  AbstractPlanState *outer_plan_state = outerAbstractPlanState(result_state);

  return PlanTransformer::TransformPlan(outer_plan_state);
}

}  // namespace bridge
}  // namespace peloton
