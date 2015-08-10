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

  // Ignore empty plans
  if (planstate == nullptr) return nullptr;

  planner::AbstractPlan *peloton_plan = nullptr;

  switch (nodeTag(planstate)) {
    case T_ModifyTableState:
      peloton_plan = PlanTransformer::TransformModifyTable(
          reinterpret_cast<planner::ModifyTablePlanState *>(planstate),
          options);
      break;
    case T_SeqScanState:
      peloton_plan = PlanTransformer::TransformSeqScan(
          reinterpret_cast<planner::SeqScanPlanState *>(planstate),
          options);
      break;
    case T_IndexScanState:
      peloton_plan = PlanTransformer::TransformIndexScan(
          reinterpret_cast<planner::IndexScanPlanState *>(planstate),
          options);
      break;
    case T_IndexOnlyScanState:
      peloton_plan = PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<planner::IndexOnlyScanPlanState *>(planstate),
          options);
      break;
    case T_BitmapHeapScanState:
      peloton_plan = PlanTransformer::TransformBitmapScan(
          reinterpret_cast<planner::BitmapHeapScanPlanState *>(planstate),
          options);
      break;
    case T_LockRowsState:
      peloton_plan = PlanTransformer::TransformLockRows(
          reinterpret_cast<planner::LockRowsPlanState *>(planstate));
      break;
    case T_LimitState:
      peloton_plan = PlanTransformer::TransformLimit(
          reinterpret_cast<planner::LimitPlanState *>(planstate));
      break;
    case T_MergeJoinState:
    case T_HashJoinState:
    // TODO :: 'MergeJoin'/'HashJoin' have not been implemented yet, however, we
    // need this case to operate AlterTable
    // Also - Added special case in peloton_process_dml
    case T_NestLoopState:
      peloton_plan = PlanTransformer::TransformNestLoop(
          reinterpret_cast<planner::NestLoopPlanState *>(planstate));
      break;
    case T_MaterialState:
      peloton_plan = PlanTransformer::TransformMaterialization(
          reinterpret_cast<planner::MaterialPlanState *>(planstate));
      break;
    default: {
      LOG_ERROR("Unsupported Postgres Plan  Tag: %u ",
                nodeTag(planstate));
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
