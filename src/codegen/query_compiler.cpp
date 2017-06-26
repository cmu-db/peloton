//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_compiler.cpp
//
// Identification: src/codegen/query_compiler.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"

#include "codegen/compilation_context.h"
#include "planner/seq_scan_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/hash_join_plan.h"

namespace peloton {
namespace codegen {

// Constructor
QueryCompiler::QueryCompiler() : next_id_(0) {}

// Compile the given query statement
std::unique_ptr<Query> QueryCompiler::Compile(
    const planner::AbstractPlan &root, QueryResultConsumer &result_consumer,
    CompileStats *stats) {
  // The query statement we compile
  std::unique_ptr<Query> query{new Query(root)};

  // Set up the compilation context
  CompilationContext context{*query, result_consumer};

  // Perform the compilation
  context.GeneratePlan(stats);

  // Return the compiled query statement
  return query;
}

bool QueryCompiler::IsSupported(const planner::AbstractPlan &plan) {
  return QueryCompiler::IsSupported(plan, nullptr);
}

// Check if the given query can be compiled. This search is not exhaustive ...
bool QueryCompiler::IsSupported(const planner::AbstractPlan &plan,
                                const planner::AbstractPlan *parent) {
  switch (plan.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN:
    case PlanNodeType::ORDERBY:
    case PlanNodeType::DELETE:
    case PlanNodeType::AGGREGATE_V2: {
      break;
    }
    case PlanNodeType::PROJECTION: {
      if(plan.GetChildren().size() == 0)
        return false;
      break;
    }
    case PlanNodeType::HASHJOIN: {
      const auto &hjp = static_cast<const planner::HashJoinPlan &>(plan);
      // Right now, only support inner joins
      if (hjp.GetJoinType() == JoinType::INNER) {
        break;
      }
    }
    case PlanNodeType::HASH: {
      // Right now, only support hash's in hash-joins
      if (parent != nullptr &&
          parent->GetPlanNodeType() == PlanNodeType::HASHJOIN) {
        break;
      }
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
    default: {
      break;
    }
  }

  if (pred != nullptr && !IsExpressionSupported(*pred)) {
    return false;
  }

  // Check all children
  for (const auto &child : plan.GetChildren()) {
    if (!IsSupported(*child, &plan)) {
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
    case ExpressionType::FUNCTION:
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
