//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_hash_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_hash_join.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/bridge/ddl/schema_transformer.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// HashJoin
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres HashState into a Peloton HashPlanNode
 * @return Pointer to the constructed AbstractPlan
 */
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformHashJoin(
    const HashJoinPlanState *hj_plan_state) {
  std::unique_ptr<planner::AbstractPlan> result;
  PelotonJoinType join_type =
      PlanTransformer::TransformJoinType(hj_plan_state->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", hj_plan_state->jointype);
    return nullptr;
  }

  LOG_INFO("Handle hash join with join type: %d", join_type);

  /*
  std::vector<planner::HashJoinPlan::JoinClause> join_clauses(
      BuildHashJoinClauses(mj_plan_state->mj_Clauses,
                            mj_plan_state->mj_NumClauses);
  */

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(hj_plan_state->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(hj_plan_state->qual));

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

  project_info.reset(BuildProjectInfoFromTLSkipJunk(hj_plan_state->targetlist));

  if (project_info.get() != nullptr) {
    LOG_INFO("%s", project_info.get()->Debug().c_str());
  } else {
    LOG_INFO("empty projection info");
  }

  std::shared_ptr<const catalog::Schema> project_schema(
      SchemaTransformer::GetSchemaFromTupleDesc(
          hj_plan_state->tts_tupleDescriptor));

  std::vector<oid_t> outer_hashkeys =
      BuildColumnListFromExpStateList(hj_plan_state->outer_hashkeys);

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

  std::unique_ptr<planner::HashJoinPlan> plan_node(new planner::HashJoinPlan(
      join_type, std::move(predicate), std::move(project_info), project_schema,
      outer_hashkeys));

  std::unique_ptr<planner::AbstractPlan> outer{std::move(
      PlanTransformer::TransformPlan(outerAbstractPlanState(hj_plan_state)))};
  std::unique_ptr<planner::AbstractPlan> inner{std::move(
      PlanTransformer::TransformPlan(innerAbstractPlanState(hj_plan_state)))};

  /* Add the children nodes */
  plan_node->AddChild(std::move(outer));
  plan_node->AddChild(std::move(inner));

  if (non_trivial) {
    result->AddChild(std::move(plan_node));
  } else {
    result.reset(plan_node.release());
  }

  LOG_INFO("Finishing mapping Hash join, JoinType: %d", join_type);
  return result;
}

}  // namespace bridge
}  // namespace peloton
