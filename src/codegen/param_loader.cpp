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

#include <tuple>
#include "expression/abstract_expression.h"
#include "planner/seq_scan_plan.h"
#include "codegen/param_loader.h"
#include "common/logger.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

std::tuple<
    std::unordered_map<const expression::AbstractExpression *, size_t>,
    std::unordered_map<int, size_t>,
    std::vector<Parameter>>
ParamLoader::LoadParams(const planner::AbstractPlan *plan) {
  ParamLoader loader;
  loader.Load(plan);
  return std::make_tuple(
      std::move(loader.const_ids_),
      std::move(loader.value_ids_),
      std::move(loader.params_));
}

void ParamLoader::Load(const planner::AbstractPlan *plan) {
  switch (plan->GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      Load(static_cast<const planner::SeqScanPlan *>(plan));
      break;
    }
    case PlanNodeType::PROJECTION: {
      Load(static_cast<const planner::ProjectionPlan *>(plan));
      break;
    }
    case PlanNodeType::HASHJOIN: {
      Load(static_cast<const planner::HashJoinPlan *>(plan));
      break;
    }
    case PlanNodeType::AGGREGATE_V2: {
      Load(static_cast<const planner::AggregatePlan *>(plan));
      break;
    }
    case PlanNodeType::ORDERBY: {
      Load(static_cast<const planner::OrderByPlan *>(plan));
      break;
    }
    case PlanNodeType::DELETE: {
      Load(static_cast<const planner::DeletePlan *>(plan));
      break;
    }
    case PlanNodeType::UPDATE: {
      Load(static_cast<const planner::UpdatePlan *>(plan));
      break;
    }
    case PlanNodeType::INSERT: {
      Load(static_cast<const planner::InsertPlan *>(plan));
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
void ParamLoader::Load(const planner::SeqScanPlan *plan) {
  auto predicate = plan->GetPredicate();
  if (predicate != nullptr) {
    Load(predicate);
  }
}

/**
 * ProjectionPlan :-
 *   child : Plan,
 *   proj  : ProjectInfo
 */
void ParamLoader::Load(const planner::ProjectionPlan *plan) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0));
  Load(plan->GetProjectInfo());
}

/**
 * HashJoinPlan :-
 *   left            : Plan,
 *   right           : Plan,
 *   left_hash_keys  : [Expr],
 *   right_hash_keys : [Expr]
 */
void ParamLoader::Load(const planner::HashJoinPlan *plan) {
  PL_ASSERT(plan->GetChildren().size() == 2);

  Load(plan->GetChild(0));
  Load(plan->GetChild(1)->GetChild(0));

  std::vector<const expression::AbstractExpression *> left_hash_keys;
  plan->GetLeftHashKeys(left_hash_keys);
  for (auto left_hash_key : left_hash_keys) {
    Load(left_hash_key);
  }

  std::vector<const expression::AbstractExpression *> right_hash_keys;
  plan->GetLeftHashKeys(right_hash_keys);
  for (auto right_hash_key : right_hash_keys) {
    Load(right_hash_key);
  }

  auto predicate = plan->GetPredicate();
  if (predicate != nullptr) {
    Load(predicate);
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
void ParamLoader::Load(const planner::AggregatePlan *plan) {
  // @see translator_factory.cpp
  // An aggregation without any grouping clause is simpler to handle. All
  // other aggregations are handled using a hash-group-by for now.
  // TODO: Implement other (non hash) group-by algorithms

  // NOTE: This is mimicking the translator factory.
  // It might be better to squeeze the two cases into one.

  if (plan->GetGroupbyColIds().size() == 0) {
    // GlobalGroupByPlan

    Load(plan->GetChild(0));

    for (const auto &agg_term : plan->GetUniqueAggTerms()) {
      if (agg_term.expression != nullptr) {
        Load(agg_term.expression);
      }
    }

  } else {
    // HashGroupBy

    Load(plan->GetChild(0));

    auto predicate = plan->GetPredicate();
    if (predicate != nullptr) {
      Load(predicate);
    }

    for (const auto &agg_term : plan->GetUniqueAggTerms()) {
      if (agg_term.expression != nullptr) {
        Load(agg_term.expression);
      }
    }

    auto projection = plan->GetProjectInfo();
    if (projection != nullptr) {
      Load(projection);
    }
  }
}

/**
 * OrderByPlan :-
 *   child : Plan,
 *   order_keys : [ColumnID],
 *   outputs : [ColumnID]
 */
void ParamLoader::Load(const planner::OrderByPlan *plan) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0));
}

/**
 * DeletePlan :-
 *   child : Plan
 */
void ParamLoader::Load(const planner::DeletePlan *plan) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0));
}

/**
 * UpdatePlan :-
 *   child : Plan,
 *   proj  : ProjectInfo
 */
void ParamLoader::Load(const planner::UpdatePlan *plan) {
  PL_ASSERT(plan->GetChildren().size() == 1);
  Load(plan->GetChild(0));

  auto projection = plan->GetProjectInfo();
  if (projection != nullptr) {
    Load(projection);
  }
}

/**
 * InsertPlan :-
 *
 */
void ParamLoader::Load(const planner::InsertPlan *plan) {
  if (plan->GetChildren().size() == 1) {
    // InsertScan

    Load(plan->GetChild(0));

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

void ParamLoader::Load(const expression::AbstractExpression *expr) {
  for (int i = 0; i < static_cast<int>(expr->GetChildrenSize()); ++i) {
    Load(expr->GetChild(i));
  }

  switch (expr->GetExpressionType()) {
    case ExpressionType::VALUE_PARAMETER: {
      Load(static_cast<const expression::ParameterValueExpression *>(expr));
      break;
    }
    case ExpressionType::VALUE_CONSTANT: {
      Load(static_cast<const expression::ConstantValueExpression *>(expr));
      break;
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      Load(static_cast<const expression::CaseExpression *>(expr));
      break;
    }
    default: {
      break;
    }
  }
}

void ParamLoader::Load(const expression::ParameterValueExpression *expr) {
  int value_id = expr->GetValueIdx();
  this->value_ids_[value_id] = this->num_params_;
  this->num_params_++;
  this->params_.push_back(
      Parameter::GetParamValParamInstance(value_id, expr->GetValueType()));
}

void ParamLoader::Load(const expression::ConstantValueExpression *expr) {
  this->const_ids_[expr] = this->num_params_;
  this->num_params_++;
  this->params_.push_back(
      Parameter::GetConstValParamInstance(expr->GetValue()));
}

void ParamLoader::Load(const expression::CaseExpression *expr) {
  // We need to prepare each component of the case
  for (const auto &clause : expr->GetWhenClauses()) {
    Load(clause.first.get());
    Load(clause.second.get());
  }

  if (expr->GetDefault() != nullptr) {
    Load(expr->GetDefault());
  }
}

// =======================================
// Load parameters from a projection info.
// =======================================

/**
 * ProjectionInfo :-
 *   DirectMapList,
 *   TargetList
 */
void ParamLoader::Load(const planner::ProjectInfo *projection) {
  // If the projection is non-trivial, we need to prepare translators for every
  // target expression.
  if (projection->isNonTrivial()) {
    for (const auto &target : projection->GetTargetList()) {
      const auto &derived_attribute = target.second;
      PL_ASSERT(derived_attribute.expr != nullptr);
      Load(derived_attribute.expr);
    }
  }
}

}  // namespace codegen
}  // namespace peloton
