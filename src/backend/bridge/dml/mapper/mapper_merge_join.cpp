//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_merge_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_merge_join.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
const std::shared_ptr<planner::AbstractPlan>
PlanTransformer::TransformMergeJoin(const MergeJoinPlanState *mj_plan_state) {
  std::shared_ptr<planner::AbstractPlan> result;
  std::shared_ptr<planner::MergeJoinPlan> plan_node;
  PelotonJoinType join_type =
      PlanTransformer::TransformJoinType(mj_plan_state->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", mj_plan_state->jointype);
    return std::shared_ptr<planner::AbstractPlan>();
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
    predicate = expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_AND, join_filter, plan_filter);
  } else if (join_filter) {
    predicate = join_filter;
  } else {
    predicate = plan_filter;
  }

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  std::shared_ptr<catalog::Schema> project_schema(
      SchemaTransformer::GetSchemaFromTupleDesc(
          mj_plan_state->tts_tupleDescriptor));

  project_info.reset(BuildProjectInfoFromTLSkipJunk(mj_plan_state->targetlist));

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");
    result = std::make_shared<planner::ProjectionPlan>(project_info.release(),
                                                       project_schema);
    plan_node = std::make_shared<planner::MergeJoinPlan>(
        join_type, predicate, nullptr, project_schema, join_clauses);
    result.get()->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node = std::make_shared<planner::MergeJoinPlan>(
        join_type, predicate, project_info.release(), project_schema,
        join_clauses);
    result = plan_node;
  }

  std::shared_ptr<planner::AbstractPlan> outer =
      PlanTransformer::TransformPlan(outerAbstractPlanState(mj_plan_state));
  std::shared_ptr<planner::AbstractPlan> inner =
      PlanTransformer::TransformPlan(innerAbstractPlanState(mj_plan_state));

  /* Add the children nodes */
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

    LOG_INFO("left: %s", clause.left_->Debug(" ").c_str());
    LOG_INFO("right: %s", clause.right_->Debug(" ").c_str());

    clauses.push_back(std::move(clause));
  }
  LOG_INFO("Build join clauses of size %lu", clauses.size());
  return clauses;
}
}  // namespace bridge
}  // namespace peloton
