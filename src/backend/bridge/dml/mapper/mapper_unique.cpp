//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_unique.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_unique.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// mapper_unique
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres Unique into a Peloton Node.
 *
 *    currently, we just return the
 *    underlying node
 *
 * @return Pointer to the constructed AbstractPlan.
 *
 */
const planner::AbstractPlan *PlanTransformer::TransformUnique(
    const UniquePlanState *unique_plan_state) {

  LOG_INFO("Handle Unique");

  // get the underlying plan
  AbstractPlanState *outer_plan_state = outerAbstractPlanState(unique_plan_state);

  return PlanTransformer::TransformPlan(outer_plan_state);

}

}  // namespace bridge
}  // namespace peloton
