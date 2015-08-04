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
#include "backend/planner/nested_loop_join_node.h"
#include "backend/planner/projection_node.h"
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
planner::AbstractPlanNode* PlanTransformer::TransformNestLoop(
    const NestLoopState* nl_plan_state) {
  const JoinState *js = &(nl_plan_state->js);
  planner::AbstractPlanNode *parent = nullptr;
  planner::AbstractPlanNode *result = nullptr;
  PelotonJoinType join_type = PlanTransformer::TransformJoinType(js->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", js->jointype);
    return nullptr;
  }

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
    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(nl_plan_state->js.ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    parent = new planner::ProjectionNode(project_info.release(), project_schema);
  }


  planner::AbstractPlanNode *outer = PlanTransformer::TransformPlan(outerPlanState(nl_plan_state));
  planner::AbstractPlanNode *inner = PlanTransformer::TransformPlan(innerPlanState(nl_plan_state));

  /* Construct and return the Peloton plan node */
  auto plan_node = new planner::NestedLoopJoinNode(predicate, project_info.release());
  plan_node->SetJoinType(join_type);
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);



  if (parent) {
    parent->AddChild(plan_node);
    result = parent;
  } else {
    result = plan_node;
  }
  LOG_INFO("Handle Nested loop join, JoinType: %d", join_type);
  return result;
}

}  // namespace bridge
}  // namespace peloton
