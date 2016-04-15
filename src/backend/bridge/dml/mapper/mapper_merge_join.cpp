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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformMergeJoin(
    const MergeJoinPlanState *mj_plan_state) {
  std::unique_ptr<planner::AbstractPlan> result;
  PelotonJoinType join_type =
      PlanTransformer::TransformJoinType(mj_plan_state->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", mj_plan_state->jointype);
    return std::unique_ptr<planner::AbstractPlan>();
  }

  LOG_INFO("Handle merge join with join type: %d", join_type);

  std::vector<planner::MergeJoinPlan::JoinClause> join_clauses =
      BuildMergeJoinClauses(mj_plan_state->mj_Clauses,
                            mj_plan_state->mj_NumClauses);

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(mj_plan_state->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(mj_plan_state->qual));

  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);
  if (join_filter && plan_filter) {
    predicate.reset(expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_AND, join_filter, plan_filter));
  } else if (join_filter) {
    predicate.reset(join_filter);
  } else {
    predicate.reset(plan_filter);
  }

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  std::shared_ptr<const catalog::Schema> project_schema(
      SchemaTransformer::GetSchemaFromTupleDesc(
          mj_plan_state->tts_tupleDescriptor));

  project_info.reset(BuildProjectInfoFromTLSkipJunk(mj_plan_state->targetlist));

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

  std::unique_ptr<planner::MergeJoinPlan> plan_node(new planner::MergeJoinPlan(
      join_type, std::move(predicate), std::move(project_info), project_schema,
      join_clauses));

  std::unique_ptr<planner::AbstractPlan> outer{std::move(
      PlanTransformer::TransformPlan(outerAbstractPlanState(mj_plan_state)))};
  std::unique_ptr<planner::AbstractPlan> inner{std::move(
      PlanTransformer::TransformPlan(innerAbstractPlanState(mj_plan_state)))};

  /* Add the children nodes */
  plan_node->AddChild(std::move(outer));
  plan_node->AddChild(std::move(inner));

  if (non_trivial) {
    result->AddChild(std::move(plan_node));
  } else {
    result.reset(plan_node.release());
  }

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
