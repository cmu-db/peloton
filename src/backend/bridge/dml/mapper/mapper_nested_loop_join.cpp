//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_nested_loop_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_nested_loop_join.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/nested_loop_join_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/bridge/ddl/schema_transformer.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Nested Loop Join
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres NestLoop into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
const planner::AbstractPlan *PlanTransformer::TransformNestLoop(
    const NestLoopPlanState *nl_plan_state) {
  PelotonJoinType peloton_join_type =
      PlanTransformer::TransformJoinType(nl_plan_state->jointype);

  NestLoop *nl = nl_plan_state->nl;

  if (peloton_join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", nl_plan_state->jointype);
    return nullptr;
  }

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(nl_plan_state->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(nl_plan_state->qual));

  expression::AbstractExpression *predicate = nullptr;
  if (join_filter && plan_filter) {
    predicate = expression::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                               join_filter, plan_filter);
  } else if (join_filter) {
    predicate = join_filter;
  } else {
    predicate = plan_filter;
  }

  // TODO: do we need to consider target list here?
  // Transform project info
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  project_info.reset(BuildProjectInfo(nl_plan_state->ps_ProjInfo));

  LOG_INFO("%s", project_info.get()->Debug().c_str());

  planner::AbstractPlan *result = nullptr;
  planner::NestedLoopJoinPlan *plan_node = nullptr;

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");

    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(
        nl_plan_state->tts_tupleDescriptor);

    result =
        new planner::ProjectionPlan(project_info.release(), project_schema);
    plan_node =
    	new planner::NestedLoopJoinPlan(peloton_join_type, predicate, nullptr, nl);
    result->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node = new planner::NestedLoopJoinPlan(peloton_join_type, predicate,
                                                    project_info.release(), nl);
    result = plan_node;
  }

  const planner::AbstractPlan *outer =
      PlanTransformer::TransformPlan(outerAbstractPlanState(nl_plan_state));
  const planner::AbstractPlan *inner =
      PlanTransformer::TransformPlan(innerAbstractPlanState(nl_plan_state));

  /* Add the children nodes */
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);

  LOG_INFO("Finishing mapping Nested loop join, JoinType: %d",
           nl_plan_state->jointype);
  return result;
}

}  // namespace bridge
}  // namespace peloton
