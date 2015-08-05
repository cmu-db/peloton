//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// mapper_materialization.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_materialization.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/materialization_node.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Materialization
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres Material into a Peloton Materialization node
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *
PlanTransformer::TransformMaterialization(const MaterialState *plan_state) {

  PlanState *outer_plan_state = outerPlanState(plan_state);
  planner::AbstractPlanNode *child = TransformPlan(outer_plan_state);
  bool physify_flag =
      false; // current, we just pass the underlying plan node for this case

  planner::AbstractPlanNode *plan_node =
      new planner::MaterializationNode(physify_flag);
  plan_node->AddChild(child);

  return plan_node;
}

} // namespace bridge
} // namespace peloton
