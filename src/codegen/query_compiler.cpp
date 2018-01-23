//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_compiler.cpp
//
// Identification: src/codegen/query_compiler.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"

#include "codegen/compilation_context.h"
#include "planner/aggregate_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace codegen {

// Constructor
QueryCompiler::QueryCompiler() : next_id_(0) {}

// Compile the given query statement
std::unique_ptr<Query> QueryCompiler::Compile(
    const planner::AbstractPlan &root, const QueryParametersMap &parameters_map,
    QueryResultConsumer &result_consumer, CompileStats *stats) {
  // The query statement we compile
  std::unique_ptr<Query> query{new Query(root)};

  // Set up the compilation context
  CompilationContext context{*query, parameters_map, result_consumer};

  // Perform the compilation
  context.GeneratePlan(stats);

  // Return the compiled query statement
  return query;
}

// Check if the given query can be compiled. This search is not exhaustive ...
bool QueryCompiler::IsSupported(const planner::AbstractPlan &plan) {
  switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN:
    case PlanNodeType::ORDERBY:
    case PlanNodeType::DELETE:
    case PlanNodeType::INSERT:
    case PlanNodeType::UPDATE:
    case PlanNodeType::AGGREGATE_V2: {
      break;
    }
    case PlanNodeType::PROJECTION: {
      // TODO(pmenon): Why does this check exists?
      if (plan.GetChildren().empty()) {
        return false;
      }

      // Check each projection expression
      auto &proj_plan = static_cast<const planner::ProjectionPlan &>(plan);
      auto &proj_info = *proj_plan.GetProjectInfo();
      for (const auto &target : proj_info.GetTargetList()) {
        const planner::DerivedAttribute &attr = target.second;
        if (!IsExpressionSupported(*attr.expr)) {
          return false;
        }
      }
      break;
    }
    case PlanNodeType::NESTLOOP:
    case PlanNodeType::HASHJOIN: {
      const auto &join = static_cast<const planner::AbstractJoinPlan &>(plan);
      // Right now, only support inner joins
      if (join.GetJoinType() == JoinType::INNER) {
        break;
      }
    }
    case PlanNodeType::HASH: {
      break;
    }
    default: { return false; }
  }

  // Check the predicate is compilable
  const expression::AbstractExpression *pred = nullptr;
  switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      auto &scan_plan = static_cast<const planner::SeqScanPlan &>(plan);
      pred = scan_plan.GetPredicate();
      break;
    }
    case PlanNodeType::AGGREGATE_V2: {
      auto &agg_plan = static_cast<const planner::AggregatePlan &>(plan);
      pred = agg_plan.GetPredicate();
      break;
    }
    case PlanNodeType::HASHJOIN: {
      auto &hj_plan = static_cast<const planner::HashJoinPlan &>(plan);
      pred = hj_plan.GetPredicate();
      break;
    }
    default: { break; }
  }

  if (pred != nullptr && !IsExpressionSupported(*pred)) {
    return false;
  }

  // Check all children
  for (const auto &child : plan.GetChildren()) {
    if (!IsSupported(*child)) {
      return false;
    }
  }

  // Looks good ...
  return true;
}

bool QueryCompiler::IsExpressionSupported(
    const expression::AbstractExpression &expr) {
  switch (expr.GetExpressionType()) {
    case ExpressionType::STAR:
    case ExpressionType::VALUE_PARAMETER:
      return false;
    default:
      break;
  }

  // Check tree recursively
  for (uint32_t i = 0; i < expr.GetChildrenSize(); i++) {
    if (!IsExpressionSupported(*expr.GetChild(i))) {
      return false;
    }
  }

  // Looks good ...
  return true;
}

}  // namespace codegen
}  // namespace peloton
