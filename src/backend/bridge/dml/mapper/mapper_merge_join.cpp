/*-------------------------------------------------------------------------
 *
 * mapper_merge_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_merge_join.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/merge_join_node.h"
#include "backend/planner/projection_node.h"
#include "backend/bridge/ddl/schema_transformer.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Nested Loop Join
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres MergeJoin into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode* PlanTransformer::TransformMergeJoin(
    const MergeJoinState* mj_plan_state) {
  const JoinState *js = &(mj_plan_state->js);
  planner::AbstractPlanNode *result = nullptr;
  planner::MergeJoinNode *plan_node = nullptr;
  PelotonJoinType join_type = PlanTransformer::TransformJoinType(js->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", js->jointype);
    return nullptr;
  }

  expression::AbstractExpression *join_clause = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(mj_plan_state->mj_Clauses));

  LOG_INFO("Merge Cond: %s", join_clause->Debug(" ").c_str());

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(js->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(js->ps.qual));

  expression::AbstractExpression *predicate = nullptr;
  if (join_filter && plan_filter) {
    predicate = expression::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                     join_filter, plan_filter);
  } else if (join_filter) {
    predicate = join_filter;
  } else {
    predicate = plan_filter;
  }

  /* TODO: do we need to consider target list here? */
  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);
  project_info.reset(BuildProjectInfo(js->ps.ps_ProjInfo,
                                      js->ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts));

  LOG_INFO("\n%s", project_info.get()->Debug().c_str());

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");
    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(mj_plan_state->js.ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    result = new planner::ProjectionNode(project_info.release(), project_schema);
    plan_node = new planner::MergeJoinNode(predicate, nullptr, join_clause);
    result->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node = new planner::MergeJoinNode(predicate, project_info.release(), join_clause);
    result = plan_node;
  }

  planner::AbstractPlanNode *outer = PlanTransformer::TransformPlan(outerPlanState(mj_plan_state));
  planner::AbstractPlanNode *inner = PlanTransformer::TransformPlan(innerPlanState(mj_plan_state));

  /* Add the children nodes */
  plan_node->SetJoinType(join_type);
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);

  LOG_INFO("Finishing mapping Nested loop join, JoinType: %d", join_type);
  return result;
}

}  // namespace bridge
}  // namespace peloton

