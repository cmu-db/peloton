//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// param_loader.cpp
//
// Identification: src/codegen/param_loader.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * This file contains functions traversing plans to extract all parameters.
 */

#include "planner/seq_scan_plan.h"
#include "codegen/param_loader.h"
#include "common/logger.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

std::vector<type::Value> ParamLoader::Load(const planner::AbstractPlan *plan) {
  std::vector<type::Value> params;
  Load(plan, &params);
  return std::move(params);
}

void ParamLoader::Load(const planner::AbstractPlan *plan,
                       std::vector<type::Value> *params) {
  switch (plan->GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      Load(static_cast<const planner::SeqScanPlan *>(plan), params);
      break;
    }
    case PlanNodeType::PROJECTION: {
      Load(static_cast<const planner::ProjectionPlan *>(plan), params);
      break;
    }
    case PlanNodeType::HASHJOIN: {
      Load(static_cast<const planner::HashJoinPlan *>(plan), params);
      break;
    }
    case PlanNodeType::AGGREGATE_V2: {
      Load(static_cast<const planner::AggregatePlan *>(plan), params);
      break;
    }
    case PlanNodeType::ORDERBY: {
      Load(static_cast<const planner::OrderByPlan *>(plan), params);
      break;
    }
    case PlanNodeType::DELETE: {
      Load(static_cast<const planner::DeletePlan *>(plan), params);
      break;
    }
    case PlanNodeType::UPDATE: {
      Load(static_cast<const planner::UpdatePlan *>(plan), params);
      break;
    }
    case PlanNodeType::INSERT: {
      Load(static_cast<const planner::InsertPlan *>(plan), params);
      break;
    }
    default: {
      LOG_ERROR("Plan type %d not supported",
                static_cast<int>(plan->GetPlanNodeType()));
      PL_ASSERT(false);
    }
  }
}

/**
 * SeqScanPlan :-
 *   pred : (Expr)?
 */
void ParamLoader::Load(const planner::SeqScanPlan *plan,
                       std::vector<type::Value> *params) {
  auto predicate = plan->GetPredicate();
  if (predicate != nullptr) {
    Load(predicate, params);
  }
}

/**
 * ProjectionPlan :-
 *   child : Plan,
 *   proj  : ProjectInfo
 */
void ParamLoader::Load(const planner::ProjectionPlan *plan,
                       std::vector<type::Value> *params) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0), params);
  Load(plan->GetProjectInfo(), params);
}

/**
 * HashJoinPlan :-
 *   left            : Plan,
 *   right           : Plan,
 *   left_hash_keys  : [Expr],
 *   right_hash_keys : [Expr]
 */
void ParamLoader::Load(const planner::HashJoinPlan *plan,
                       std::vector<type::Value> *params) {
  PL_ASSERT(plan->GetChildren().size() == 2);

  Load(plan->GetChild(0), params);
  Load(plan->GetChild(1), params);

  std::vector<const expression::AbstractExpression *> left_hash_keys;
  plan->GetLeftHashKeys(left_hash_keys);
  for (auto left_hash_key : left_hash_keys) {
    Load(left_hash_key, params);
  }

  std::vector<const expression::AbstractExpression *> right_hash_keys;
  plan->GetLeftHashKeys(right_hash_keys);
  for (auto right_hash_key : right_hash_keys) {
    Load(right_hash_key, params);
  }

  auto predicate = plan->GetPredicate();
  if (predicate != nullptr) {
    Load(predicate, params);
  }
}

/**
 * AggregatePlan :-
 *   GlobalGroupByPlan
 * | HashGroupByPlan
 *
 * GlobalGroupByPlan :-
 *   child : Plan,
 *   agg_terms : [AggTerm]
 *
 * HashGroupByPlan :-
 *   child : Plan,
 *   pred  : (Expr)?,
 *   agg_terms : [AggTerm],
 *   proj  : (ProjectInfo)?
 */
void ParamLoader::Load(const planner::AggregatePlan *plan,
                       std::vector<type::Value> *params) {
  // @see translator_factory.cpp
  // An aggregation without any grouping clause is simpler to handle. All
  // other aggregations are handled using a hash-group-by for now.
  // TODO: Implement other (non hash) group-by algorithms

  // NOTE: This is mimicking the translator factory.
  // It might be better to squeeze the two cases into one.

  if (plan->GetGroupbyColIds().size() == 0) {
    // GlobalGroupByPlan

    Load(plan->GetChild(0), params);

    for (const auto &agg_term : plan->GetUniqueAggTerms()) {
      if (agg_term.expression != nullptr) {
        Load(agg_term.expression, params);
      }
    }

  } else {
    // HashGroupBy

    Load(plan->GetChild(0), params);

    auto predicate = plan->GetPredicate();
    if (predicate != nullptr) {
      Load(predicate, params);
    }

    for (const auto &agg_term : plan->GetUniqueAggTerms()) {
      if (agg_term.expression != nullptr) {
        Load(agg_term.expression, params);
      }
    }

    auto projection = plan->GetProjectInfo();
    if (projection != nullptr) {
      Load(projection, params);
    }
  }
}

/**
 * OrderByPlan :-
 *   child : Plan,
 *   order_keys : [ColumnID],
 *   outputs : [ColumnID]
 */
void ParamLoader::Load(const planner::OrderByPlan *plan,
                       std::vector<type::Value> *params) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0), params);
}

/**
 * DeletePlan :-
 *   child : Plan
 */
void ParamLoader::Load(const planner::DeletePlan *plan,
                       std::vector<type::Value> *params) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0), params);
}

/**
 * UpdatePlan :-
 *   child : Plan,
 *   proj  : ProjectInfo
 */
void ParamLoader::Load(const planner::UpdatePlan *plan,
                       std::vector<type::Value> *params) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0), params);

  auto projection = plan->GetProjectInfo();
  if (projection != nullptr) {
    Load(projection, params);
  }
}

/**
 * InsertPlan :-
 *
 */
void ParamLoader::Load(const planner::InsertPlan *plan,
                       std::vector<type::Value> *params) {
  if (plan->GetChildren().size() == 1) {
    // InsertScan

    Load(plan->GetChild(0), params);

  } else if (plan->GetChildren().size() == 0) {
    // InsertRaw

    // TODO: Implement this!

  } else {
    PL_ASSERT("Wrong number of children");
  }
}

// ===================================
// Load parameters from an expression.
// ===================================

void ParamLoader::Load(const expression::AbstractExpression *expr,
                       std::vector<type::Value> *params) {
  for (size_t i = 0; i != expr->GetChildrenSize(); ++i) {
    Load(expr->GetChild(i), params);
  }

  switch (expr->GetExpressionType()) {
    case ExpressionType::VALUE_PARAMETER: {
      Load(static_cast<const expression::ParameterValueExpression *>(expr),
           params);
      break;
    }
    case ExpressionType::VALUE_CONSTANT: {
      Load(static_cast<const expression::ConstantValueExpression *>(expr),
           params);
      break;
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      Load(static_cast<const expression::CaseExpression *>(expr), params);
      break;
    }
    default: {
      break;
    }
  }
}

void ParamLoader::Load(const expression::ParameterValueExpression *expr,
                       std::vector<type::Value> *params) {
  (void)expr;
  (void)params;
  // TODO: Implement this!
}

void ParamLoader::Load(const expression::ConstantValueExpression *expr,
                       std::vector<type::Value> *params) {
  (void)expr;
  (void)params;
  // TODO: Implement this!
}

void ParamLoader::Load(const expression::CaseExpression *expr,
                       std::vector<type::Value> *params) {
  (void)expr;
  (void)params;
  // TODO: Implement this!
}

// =======================================
// Load parameters from a projection info.
// =======================================

/**
 * ProjectionInfo :-
 *   DirectMapList,
 *   TargetList
 */
void ParamLoader::Load(const planner::ProjectInfo *projection,
                       std::vector<type::Value> *params) {
  // If the projection is non-trivial, we need to prepare translators for every
  // target expression.
  if (projection->isNonTrivial()) {
    for (const auto &target : projection->GetTargetList()) {
      const auto &derived_attribute = target.second;
      PL_ASSERT(derived_attribute.expr != nullptr);
      Load(derived_attribute.expr, params);
    }
  }
}

}  // namespace codegen
}  // namespace peloton
