//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_hash_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_hash_join.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
const planner::AbstractPlan* PlanTransformer::TransformHashJoin(
    const HashJoinPlanState* hj_plan_state) {


  planner::AbstractPlan *result = nullptr;
  planner::HashJoinPlan *plan_node = nullptr;
  PelotonJoinType join_type = PlanTransformer::TransformJoinType(hj_plan_state->jointype);
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

  project_info.reset(BuildProjectInfoFromTLSkipJunk(hj_plan_state->targetlist));

  LOG_INFO("\n%s", project_info.get()->Debug().c_str());

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");
    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(
        hj_plan_state->tts_tupleDescriptor);
    result = new planner::ProjectionPlan(project_info.release(),
                                         project_schema);
    plan_node = new planner::HashJoinPlan(predicate, nullptr);
    result->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node = new planner::HashJoinPlan(predicate, project_info.release());
    result = plan_node;
  }

  const planner::AbstractPlan *outer = PlanTransformer::TransformPlan(
      outerAbstractPlanState(hj_plan_state));
  const planner::AbstractPlan *inner = PlanTransformer::TransformPlan(
      innerAbstractPlanState(hj_plan_state));

  /* Add the children nodes */
  plan_node->SetJoinType(join_type);
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);

  LOG_INFO("Finishing mapping Hash join, JoinType: %d", join_type);
  return result;
}
/*
static std::vector<planner::HashJoinPlan::JoinClause> BuildHashJoinClauses(
    const HashJoinClause join_clauses, const int num_clauses) {
  HashJoinClause join_clause = join_clauses;
  expression::AbstractExpression *left = nullptr;
  expression::AbstractExpression *right = nullptr;
  std::vector<planner::HashJoinPlan::JoinClause> clauses;
  LOG_INFO("Mapping merge join clauses of size %d", num_clauses);
  for (int i = 0; i < num_clauses; ++i, ++join_clause) {
    left = ExprTransformer::TransformExpr(join_clause->lexpr);
    right = ExprTransformer::TransformExpr(join_clause->rexpr);
    planner::HashJoinPlan::JoinClause clause(left, right,
                                              join_clause->ssup.ssup_reverse);

    LOG_INFO("left: %s\nright: %s", clause.left_->Debug(" ").c_str(),
             clause.right_->Debug(" ").c_str());

    clauses.push_back(std::move(clause));

  }
  LOG_INFO("Build join clauses of size %lu", clauses.size());
  return clauses;
}
*/
}  // namespace bridge
}  // namespace peloton
