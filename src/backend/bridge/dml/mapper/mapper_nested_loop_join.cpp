//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_nested_loop_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_nested_loop_join.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformNestLoop(
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

  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);
  if (join_filter && plan_filter) {
    predicate.reset(expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_AND, join_filter, plan_filter));
  } else if (join_filter) {
    predicate.reset(join_filter);
  } else {
    predicate.reset(plan_filter);
  }

  // TODO: do we need to consider target list here?
  // Transform project info
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  project_info.reset(BuildProjectInfo(nl_plan_state->ps_ProjInfo));

  if (project_info.get() != nullptr) {
    LOG_INFO("%s", project_info.get()->Debug().c_str());
  } else {
    LOG_INFO("empty projection info");
  }

  std::unique_ptr<planner::AbstractPlan> result;
  std::shared_ptr<const catalog::Schema> project_schema(
      SchemaTransformer::GetSchemaFromTupleDesc(
          nl_plan_state->tts_tupleDescriptor));

  bool non_trivial = (project_info.get() != nullptr &&
                      project_info.get()->isNonTrivial());
  if (non_trivial) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");

    result = std::unique_ptr<planner::AbstractPlan>(
        new planner::ProjectionPlan(std::move(project_info), project_schema));
    // set project_info to nullptr
    project_info.reset();
  } else {
    LOG_INFO("We have direct mapping projection");
  }

  std::unique_ptr<planner::NestedLoopJoinPlan> plan_node(
      new planner::NestedLoopJoinPlan(peloton_join_type, std::move(predicate),
                                      std::move(project_info), project_schema,
                                      nl));

  std::unique_ptr<planner::AbstractPlan> outer{std::move(
      PlanTransformer::TransformPlan(outerAbstractPlanState(nl_plan_state)))};
  std::unique_ptr<planner::AbstractPlan> inner{std::move(
      PlanTransformer::TransformPlan(innerAbstractPlanState(nl_plan_state)))};

  /* Add the children nodes */
  plan_node->AddChild(std::move(outer));
  plan_node->AddChild(std::move(inner));

  if (non_trivial) {
    result->AddChild(std::move(plan_node));
  } else {
    result.reset(plan_node.release());
  }

  LOG_INFO("Finishing mapping Nested loop join, JoinType: %d",
           nl_plan_state->jointype);

  return result;
}

}  // namespace bridge
}  // namespace peloton
