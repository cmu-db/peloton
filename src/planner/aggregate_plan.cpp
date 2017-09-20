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

}  // namespace planner
}  // namespace peloton
