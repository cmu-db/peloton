//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.cpp
//
// Identification: src/planner/aggregate_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/aggregate_plan.h"

#include "codegen/type/bigint_type.h"
#include "codegen/type/decimal_type.h"
#include "common/logger.h"

namespace peloton {
namespace planner {

AggregatePlan::AggTerm::AggTerm(ExpressionType et,
                                expression::AbstractExpression *expr,
                                bool distinct)
    : aggtype(et), expression(expr), distinct(distinct) {}

void AggregatePlan::AggTerm::PerformBinding(BindingContext &binding_context) {
  // If there's an input expression, first perform binding
  auto *agg_expr = const_cast<expression::AbstractExpression *>(expression);
  if (agg_expr != nullptr) {
    agg_expr->PerformBinding({&binding_context});
  }

  // Setup the aggregate's return type
  switch (aggtype) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // The SQL type of COUNT() or COUNT(*) is always a non-nullable BIGINT
      agg_ai.type = codegen::type::Type{codegen::type::BigInt::Instance()};
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
      // AVG() must have an input expression (that has been bound earlier).
      // The return type of the AVG() aggregate is always a SQL DECIMAL that
      // may or may not be NULL depending on the input expression.
      PL_ASSERT(expression != nullptr);
      // TODO: Move this logic into the SQL type
      const auto &input_type = expression->ResultType();
      agg_ai.type = codegen::type::Type{codegen::type::Decimal::Instance(),
                                        input_type.nullable};
      break;
    }
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_SUM: {
      // These aggregates must have an input expression and takes on the same
      // return type as its input expression.
      PL_ASSERT(expression != nullptr);
      agg_ai.type = expression->ResultType();
      break;
    }
    default: {
      throw Exception{StringUtil::Format(
          "%s not a valid aggregate", ExpressionTypeToString(aggtype).c_str())};
    }
  }
}

AggregatePlan::AggTerm AggregatePlan::AggTerm::Copy() const {
  return AggTerm(aggtype, expression->Copy(), distinct);
}

void AggregatePlan::PerformBinding(BindingContext &binding_context) {
  BindingContext input_context;

  const auto &children = GetChildren();
  PL_ASSERT(children.size() == 1);

  children[0]->PerformBinding(input_context);

  PL_ASSERT(groupby_ais_.empty());

  // First get bindings for the grouping keys
  for (oid_t gb_col_id : GetGroupbyColIds()) {
    auto *ai = input_context.Find(gb_col_id);
    LOG_DEBUG("Grouping col %u binds to AI %p", gb_col_id, ai);
    PL_ASSERT(ai != nullptr);
    groupby_ais_.push_back(ai);
  }

  const auto &aggregates = GetUniqueAggTerms();

  // Now let the aggregate expressions do their bindings
  for (const auto &agg_term : GetUniqueAggTerms()) {
    auto &non_const_agg_term = const_cast<AggregatePlan::AggTerm &>(agg_term);
    non_const_agg_term.PerformBinding(input_context);
  }

  // Handle the projection by creating two binding contexts, the first being
  // input context we receive, and the next being all the aggregates this
  // plan produces.

  BindingContext agg_ctx;
  for (oid_t i = 0; i < aggregates.size(); i++) {
    const auto &agg_ai = aggregates[i].agg_ai;
    LOG_DEBUG("Binding aggregate at position %u to AI %p", i, &agg_ai);
    agg_ctx.BindNew(i, &agg_ai);
  }

  // Let the predicate do its binding (if one exists)
  const auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    const_cast<expression::AbstractExpression *>(predicate)
        ->PerformBinding({&input_context, &agg_ctx});
  }

  // Do projection (if one exists)
  if (GetProjectInfo() != nullptr) {
    std::vector<const BindingContext *> inputs = {&input_context, &agg_ctx};
    GetProjectInfo()->PerformRebinding(binding_context, inputs);
  }
}

hash_t AggregatePlan::Hash(
    const std::vector<planner::AggregatePlan::AggTerm> &agg_terms) const {
  hash_t hash = 0;

  for (auto &agg_term : agg_terms) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&agg_term.aggtype));

    if (agg_term.expression != nullptr)
      hash = HashUtil::CombineHashes(hash, agg_term.expression->Hash());

    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&agg_term.distinct));
  }
  return hash;
}

hash_t AggregatePlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  hash = HashUtil::CombineHashes(hash, Hash(GetUniqueAggTerms()));

  if (GetProjectInfo() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetProjectInfo()->Hash());
  }

  for (const oid_t gb_col_id : GetGroupbyColIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&gb_col_id));
  }

  hash = HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  auto agg_strategy = GetAggregateStrategy();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&agg_strategy));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool AggregatePlan::AreEqual(
    const std::vector<planner::AggregatePlan::AggTerm> &A,
    const std::vector<planner::AggregatePlan::AggTerm> &B) const {
  if (A.size() != B.size())
    return false;

  for (size_t i = 0; i < A.size(); i++) {
    if (A[i].aggtype != B[i].aggtype)
      return false;

    auto *expr = A[i].expression;

    if (expr && (*expr != *B[i].expression))
      return false;

    if (A[i].distinct != B[i].distinct)
      return false;
  }
  return true;
}

bool AggregatePlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::AggregatePlan &>(rhs);

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr))
    return false;
  if (pred && *pred != *other_pred)
    return false;

  // UniqueAggTerms
  if (!AreEqual(GetUniqueAggTerms(), other.GetUniqueAggTerms()))
    return false;

  // Project Info
  auto *proj_info = GetProjectInfo();
  auto *other_proj_info = other.GetProjectInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr))
    return false;
  if (proj_info && *proj_info != *other_proj_info)
    return false;

  // Group by
  size_t group_by_col_ids_count = GetGroupbyColIds().size();
  if (group_by_col_ids_count != other.GetGroupbyColIds().size())
    return false;

  for (size_t i = 0; i < group_by_col_ids_count; i++) {
    if (GetGroupbyColIds()[i] != other.GetGroupbyColIds()[i])
      return false;
  }

  if (*GetOutputSchema() != *other.GetOutputSchema())
    return false;

  if (GetAggregateStrategy() != other.GetAggregateStrategy())
    return false;

  return (AbstractPlan::operator==(rhs));
}

void AggregatePlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  for (const auto &agg_term : GetUniqueAggTerms()) {
    if (agg_term.expression != nullptr) {
      auto *expr =
          const_cast<expression::AbstractExpression *>(agg_term.expression);
      expr->VisitParameters(map, values, values_from_user);
    }
  }

  if (GetGroupbyColIds().size() > 0) {
    auto *predicate =
        const_cast<expression::AbstractExpression *>(GetPredicate());
    if (predicate != nullptr) {
      predicate->VisitParameters(map, values, values_from_user);
    }

    auto *proj_info = const_cast<planner::ProjectInfo *>(GetProjectInfo());
    if (proj_info != nullptr) {
      proj_info->VisitParameters(map, values, values_from_user);
    }
  }
}

}  // namespace planner
}  // namespace peloton
