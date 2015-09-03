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
#include "backend/planner/merge_join_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/bridge/ddl/schema_transformer.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Nested Loop Join
//===--------------------------------------------------------------------===//
static std::vector<planner::MergeJoinPlan::JoinClause> BuildMergeJoinClauses(
    const MergeJoinClause join_clauses, const int num_clauses);

/**
 * @brief Convert a Postgres MergeJoin into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlan* PlanTransformer::TransformMergeJoin(
    const MergeJoinPlanState* mj_plan_state) {


  planner::AbstractPlan *result = nullptr;
  planner::MergeJoinPlan *plan_node = nullptr;
  PelotonJoinType join_type = PlanTransformer::TransformJoinType(mj_plan_state->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", mj_plan_state->jointype);
    return nullptr;
  }

  LOG_INFO("Handle merge join with join type: %d", join_type);

  std::vector<planner::MergeJoinPlan::JoinClause> join_clauses =
      BuildMergeJoinClauses(mj_plan_state->mj_Clauses,
                            mj_plan_state->mj_NumClauses);

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(mj_plan_state->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(mj_plan_state->qual));

  expression::AbstractExpression *predicate = nullptr;
  if (join_filter && plan_filter) {
    predicate = expression::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                               join_filter, plan_filter);
  } else if (join_filter) {
    predicate = join_filter;
  } else {
    predicate = plan_filter;
  }

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  project_info.reset(BuildProjectInfoFromTLSkipJunk(mj_plan_state->targetlist));

  LOG_INFO("\n%s", project_info.get()->Debug().c_str());

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");
    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(
        mj_plan_state->tts_tupleDescriptor);
    result = new planner::ProjectionPlan(project_info.release(),
                                         project_schema);
    plan_node = new planner::MergeJoinPlan(predicate, nullptr, join_clauses);
    result->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node = new planner::MergeJoinPlan(predicate, project_info.release(),
                                           join_clauses);
    result = plan_node;
  }

  planner::AbstractPlan *outer = PlanTransformer::TransformPlan(
      outerAbstractPlanState(mj_plan_state));
  planner::AbstractPlan *inner = PlanTransformer::TransformPlan(
      innerAbstractPlanState(mj_plan_state));

  /* Add the children nodes */
  plan_node->SetJoinType(join_type);
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);

  LOG_INFO("Finishing mapping Merge join, JoinType: %d", join_type);
  return result;
}

static std::vector<planner::MergeJoinPlan::JoinClause> BuildMergeJoinClauses(
    const MergeJoinClause join_clauses, const int num_clauses) {
  MergeJoinClause join_clause = join_clauses;
  expression::AbstractExpression *left = nullptr;
  expression::AbstractExpression *right = nullptr;
  std::vector<planner::MergeJoinPlan::JoinClause> clauses;
  LOG_INFO("Mapping merge join clauses of size %d", num_clauses);
  for (int i = 0; i < num_clauses; ++i, ++join_clause) {
    left = ExprTransformer::TransformExpr(join_clause->lexpr);
    right = ExprTransformer::TransformExpr(join_clause->rexpr);
    planner::MergeJoinPlan::JoinClause clause(left, right,
                                              join_clause->ssup.ssup_reverse);

    LOG_INFO("left: %s\nright: %s", clause.left_->Debug(" ").c_str(),
             clause.right_->Debug(" ").c_str());

    clauses.push_back(std::move(clause));

  }
  LOG_INFO("Build join clauses of size %lu", clauses.size());
  return clauses;
}
}  // namespace bridge
}  // namespace peloton

