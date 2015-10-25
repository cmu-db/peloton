//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_lock_rows.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_lock_rows.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Seq Scan
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres LockRow into a Peloton Node.
 *
 *    currently, we just ignore the lock step, and return the
 *    underlying node
 *
 * @return Pointer to the constructed AbstractPlan.
 *
 */
const planner::AbstractPlan *PlanTransformer::TransformLockRows(
    const LockRowsPlanState *lr_plan_state) {

  LOG_INFO("Handle LockRows");

  // get the underlying plan
  AbstractPlanState *outer_plan_state = outerAbstractPlanState(lr_plan_state);

  return PlanTransformer::TransformPlan(outer_plan_state);

}

}  // namespace bridge
}  // namespace peloton
