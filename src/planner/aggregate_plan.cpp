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

#include "common/logger.h"

namespace peloton {
namespace planner {

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
  for (oid_t i = 0; i < aggregates.size(); i++) {
    auto &term = const_cast<planner::AggregatePlan::AggTerm &>(aggregates[i]);
    auto *term_exp =
        const_cast<expression::AbstractExpression *>(aggregates[i].expression);
    if (term_exp != nullptr) {
      term_exp->PerformBinding({&input_context});
      term.agg_ai.nullable = term_exp->IsNullable();
      if (term.aggtype == ExpressionType::AGGREGATE_AVG) {
        term.agg_ai.type = type::TypeId::DECIMAL;
      } else {
        term.agg_ai.type = term_exp->GetValueType();
      }
    } else {
      PL_ASSERT(term.aggtype == ExpressionType::AGGREGATE_COUNT_STAR);
      term.agg_ai.type = type::TypeId::BIGINT;
    }
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

}  // namespace planner
}  // namespace peloton