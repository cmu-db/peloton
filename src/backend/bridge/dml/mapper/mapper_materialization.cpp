//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_materialization.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_materialization.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/planner/materialization_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Materialization
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres Material into a Peloton Materialization node
 * @return Pointer to the constructed AbstractPlan.
 */
planner::AbstractPlan *PlanTransformer::TransformMaterialization(
    planner::MaterialPlanState *plan_state) {

  auto children = plan_state->GetChildren();
  planner::AbstractPlanState *outer_plan_state = children[0];
  planner::AbstractPlan *child = TransformPlan(outer_plan_state);

  bool physify_flag =
      false;  // current, we just pass the underlying plan node for this case

  planner::AbstractPlan *plan_node =
      new planner::MaterializationPlan(physify_flag);
  plan_node->AddChild(child);

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
