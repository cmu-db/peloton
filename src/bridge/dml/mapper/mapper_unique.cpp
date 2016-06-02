//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_unique.cpp
//
// Identification: src/bridge/dml/mapper/mapper_unique.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge/dml/mapper/mapper.h"
#include "common/logger.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Mapper Unique
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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformUnique(
    const UniquePlanState *unique_plan_state) {
  LOG_TRACE("Handle Unique");

  // get the underlying plan
  AbstractPlanState *outer_plan_state =
      outerAbstractPlanState(unique_plan_state);

  return PlanTransformer::TransformPlan(outer_plan_state);
}

}  // namespace bridge
}  // namespace peloton
