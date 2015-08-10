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
planner::AbstractPlan *PlanTransformer::TransformLockRows(
    planner::LockRowsPlanState *lr_planstate) {
  assert(nodeTag(lr_planstate) == T_LockRowsState);

  LOG_INFO("Handle LockRows");

  /* get the underlying plan */
  auto children = lr_planstate->GetChildren();
  planner::AbstractPlanState *outer_plan_state = children[0];

  TransformOptions options = kDefaultOptions;
  options.use_projInfo = false;

  return PlanTransformer::TransformPlan(outer_plan_state, options);
}

}  // namespace bridge
}  // namespace peloton
