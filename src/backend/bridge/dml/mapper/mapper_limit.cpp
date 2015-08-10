//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_limit.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_limit.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/planner/limit_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres LimitState into a Peloton LimitPlanNode
 *        does not support LIMIT ALL
 *        does not support cases where there is only OFFSET
 * @return Pointer to the constructed AbstractPlan
 */
planner::AbstractPlan *PlanTransformer::TransformLimit(
    planner::LimitPlanState *planstate) {
  int64 limit = planstate->limit;
  int64 offset = planstate->offset;

  /* TODO: does not do pass down bound to child node
   * In Peloton, they are both unsigned. But both of them cannot be negative,
   * The is safe */
  /* TODO: handle no limit and no offset cases, in which the corresponding value
   * is 0 */
  LOG_INFO("Limit: %ld, Offset: %ld", limit, offset);
  auto plan_node = new planner::LimitPlan(limit, offset);

  /* Resolve child plan */
  auto children = planstate->GetChildren();
  planner::AbstractPlanState *child_planstate = children[0];

  assert(child_planstate != nullptr);
  plan_node->AddChild(PlanTransformer::TransformPlan(child_planstate));
  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
