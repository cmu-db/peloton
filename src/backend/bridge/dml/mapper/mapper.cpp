//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <cassert>

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/planner/insert_plan.h"

#include "nodes/print.h"
#include "nodes/pprint.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"

namespace peloton {
namespace bridge {

const PlanTransformer::TransformOptions PlanTransformer::kDefaultOptions;

/**
 * @brief Pretty print the plan  tree.
 * @return none.
 */
void PlanTransformer::PrintPlan(const Plan *plan) {
}

/**
 * @brief Convert Postgres Plan (tree) into AbstractPlan (tree).
 * @return Pointer to the constructed AbstractPlan Node.
 */
planner::AbstractPlan *PlanTransformer::TransformPlan(
    planner::AbstractPlanState *planstate,
    const TransformOptions options) {

  const Plan *plan = planstate->GetPlan();
  // Ignore empty plans
  if (plan == nullptr) return nullptr;

  planner::AbstractPlan *peloton_plan = nullptr;

  switch (nodeTag(plan)) {
    case T_ModifyTable:
      peloton_plan = PlanTransformer::TransformModifyTable(
          reinterpret_cast<planner::ModifyTablePlanState *>(planstate),
          options);
      break;
    case T_SeqScan:
      peloton_plan = PlanTransformer::TransformSeqScan(
          reinterpret_cast<planner::SeqScanPlanState *>(planstate),
          options);
      break;
    case T_IndexScan:
      peloton_plan = PlanTransformer::TransformIndexScan(
          reinterpret_cast<planner::IndexScanPlanState *>(planstate),
          options);
      break;
    case T_IndexOnlyScan:
      peloton_plan = PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<planner::IndexOnlyScanPlanState *>(planstate),
          options);
      break;
    case T_BitmapHeapScan:
      peloton_plan = PlanTransformer::TransformBitmapScan(
          reinterpret_cast<planner::BitmapHeapScanPlanState *>(planstate),
          options);
      break;
    case T_LockRows:
      peloton_plan = PlanTransformer::TransformLockRows(
          reinterpret_cast<planner::LockRowsPlanState *>(planstate));
      break;
    case T_Limit:
      peloton_plan = PlanTransformer::TransformLimit(
          reinterpret_cast<planner::LimitPlanState *>(planstate));
      break;
    case T_MergeJoin:
    case T_HashJoin:
    // TODO :: 'MergeJoin'/'HashJoin' have not been implemented yet, however, we
    // need this case to operate AlterTable
    // Also - Added special case in peloton_process_dml
    case T_NestLoop:
      peloton_plan = PlanTransformer::TransformNestLoop(
          reinterpret_cast<planner::NestLoopPlanState *>(planstate));
      break;
    case T_Material:
      peloton_plan = PlanTransformer::TransformMaterialization(
          reinterpret_cast<planner::MaterialPlanState *>(planstate));
      break;
    default: {
      LOG_ERROR("Unsupported Postgres Plan  Tag: %u Plan Tag: %u ",
                nodeTag(plan), nodeTag(plan));
      elog(INFO, "Query: ");
      break;
    }
  }

  return peloton_plan;
}

/**
 * @brief Recursively destroy the nodes in a plan tree.
 */
bool PlanTransformer::CleanPlanTree(planner::AbstractPlan *root) {
  if (!root) return false;

  // Clean all children subtrees
  auto children = root->GetChildren();
  for (auto child : children) {
    auto rv = CleanPlanTree(child);
    assert(rv);
  }

  // Clean the root
  delete root;
  return true;
}

/**
 * @brief Recursively destroy the nodes in a plan state tree.
 */
bool PlanTransformer::CleanPlanStateTree(planner::AbstractPlanState *root) {
  if (!root) return false;

  // Clean all children subtrees
  auto children = root->GetChildren();
  for (auto child : children) {
    auto rv = CleanPlanStateTree(child);
    assert(rv);
  }

  // Clean the root
  delete root;
  return true;
}

}  // namespace bridge
}  // namespace peloton
