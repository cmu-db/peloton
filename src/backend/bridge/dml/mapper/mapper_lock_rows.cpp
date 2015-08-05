//===----------------------------------------------------------------------===//
//
//							PelotonDB
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
 * @return Pointer to the constructed AbstractPlanNode.
 *
 */
planner::AbstractPlanNode *
PlanTransformer::TransformLockRows(const LockRowsState *lr_plan_state) {
  assert(nodeTag(lr_plan_state) == T_LockRowsState);

  LOG_INFO("Handle LockRows");

  /* get the underlying plan */
  PlanState *outer_plan_state = outerPlanState(lr_plan_state);

  TransformOptions options = kDefaultOptions;
  options.use_projInfo = false;

  return PlanTransformer::TransformPlan(outer_plan_state, options);
}

} // namespace bridge
} // namespace peloton
