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
    : aggtype(et), expression(expr), distinct(distinct) {
  switch (et) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // For COUNT(), the type is always a non-nullable bigint
      agg_ai.type =
          codegen::type::Type{codegen::type::BigInt::Instance(), false};
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
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
      PL_ASSERT(expression != nullptr);
      agg_ai.type = expression->ResultType();
      break;
    }
    default: {
      throw Exception{StringUtil::Format("%s not a valid aggregate",
                                         ExpressionTypeToString(et))};
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
  for (oid_t i = 0; i < aggregates.size(); i++) {
    auto *term_exp =
        const_cast<expression::AbstractExpression *>(aggregates[i].expression);
    if (term_exp != nullptr) {
      term_exp->PerformBinding({&input_context});
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