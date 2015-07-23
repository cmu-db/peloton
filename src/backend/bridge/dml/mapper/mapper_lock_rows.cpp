/*-------------------------------------------------------------------------
 *
 * mapper_seq_scan.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_seq_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

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
planner::AbstractPlanNode* PlanTransformer::TransformLockRows(
    const LockRowsState* lr_plan_state) {

  assert(nodeTag(lr_plan_state) == T_LockRowsState);

  /* get the underlying plan */
  PlanState *outer_plan_state = outerPlanState(lr_plan_state);

  return PlanTransformer::TransformPlan(outer_plan_state);

}


} // namespace bridge
} // namespace peloton
