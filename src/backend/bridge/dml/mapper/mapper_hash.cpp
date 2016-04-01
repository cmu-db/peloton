//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_hash.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_hash.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/hash_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres HashState into a Peloton HashPlanNode
 * @return Pointer to the constructed AbstractPlan
 */
const std::shared_ptr<planner::AbstractPlan> PlanTransformer::TransformHash(
    const HashPlanState *hash_state) {
  auto hashkeys = ExprTransformer::TransformExprList(
      reinterpret_cast<ExprState *>(hash_state->hashkeys));

  // Resolve child plan, should be some kind of scan
  AbstractPlanState *subplan_state = outerAbstractPlanState(hash_state);
  std::shared_ptr<planner::AbstractPlan> plan_node(
      new planner::HashPlan(hashkeys));
  assert(subplan_state != nullptr);
  plan_node->AddChild(TransformPlan(subplan_state));

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
