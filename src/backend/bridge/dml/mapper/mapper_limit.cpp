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

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/limit_plan.h"

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
    const LimitPlanState *limit_state) {

  // TODO: does not do pass down bound to child node
  // TODO: handle no limit and no offset cases
  LOG_INFO("Flags :: Limit: %d, Offset: %d", limit_state->noLimit, limit_state->noOffset);
  LOG_INFO("Limit: %ld, Offset: %ld", limit_state->limit, limit_state->offset);
  auto plan_node = new planner::LimitPlan(limit_state->limit, limit_state->offset);

  // Resolve child plan
  AbstractPlanState *subplan_state = outerAbstractPlanState(limit_state);
  assert(subplan_state != nullptr);
  plan_node->AddChild(TransformPlan(subplan_state));

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
