/*-------------------------------------------------------------------------
 *
 * plan_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/plan_transformer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstring>
#include <cassert>

#include "backend/bridge/dml/mapper/mapper.h"

#include "../executor/plan_executor.h"
#include "nodes/print.h"
#include "nodes/pprint.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"

void printPlanStateTree(const PlanState * planstate);

namespace peloton {
namespace bridge {

/**
 * @brief Pretty print the plan state tree.
 * @return none.
 */
void PlanTransformer::PrintPlanState(const PlanState *plan_state) {
  printPlanStateTree(plan_state);
}

/**
 * @brief Convert Postgres PlanState (tree) into AbstractPlanNode (tree).
 * @return Pointer to the constructed AbstractPlan`Node.
 */
planner::AbstractPlanNode *PlanTransformer::TransformPlan(
    const PlanState *plan_state) {
  assert(plan_state);

  Plan *plan = plan_state->plan;
  // Ignore empty plans
  if(plan == nullptr)
    return nullptr;

  planner::AbstractPlanNode *plan_node = nullptr;

  switch (nodeTag(plan)) {
    case T_ModifyTable:
      plan_node = PlanTransformer::TransformModifyTable(
          reinterpret_cast<const ModifyTableState *>(plan_state));
      break;
    case T_SeqScan:
      plan_node = PlanTransformer::TransformSeqScan(
          reinterpret_cast<const SeqScanState*>(plan_state));
      break;
    case T_IndexScan:
      plan_node = PlanTransformer::TransformIndexScan(
          reinterpret_cast<const IndexScanState*>(plan_state));
      break;
    case T_IndexOnlyScan:
      plan_node = PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<const IndexOnlyScanState*>(plan_state));
      break;
     case T_BitmapHeapScan:
      plan_node = PlanTransformer::TransformBitmapScan(
          reinterpret_cast<const BitmapHeapScanState*>(plan_state));
      break;
    case T_LockRows:
      plan_node = PlanTransformer::TransformLockRows(
          reinterpret_cast<const LockRowsState*>(plan_state));
      break;
    case T_Limit:
      plan_node = PlanTransformer::TransformLimit(
          reinterpret_cast<const LimitState*>(plan_state));
      break;
    case T_MergeJoin:
    case T_HashJoin:
      // TODO :: 'MergeJoin'/'HashJoin' have not been implemented yet, however, we need this
      // case to operate AlterTable 
      // Also - Added special case in peloton_process_dml
      break;
    default: {
      LOG_ERROR("Unsupported Postgres Plan State Tag: %u Plan Tag: %u ",
                nodeTag(plan_state),
                nodeTag(plan));
      break;
    }
  }

  return plan_node;
}

/**
 * @brief Recursively destroy the nodes in a plan node tree.
 */
bool PlanTransformer::CleanPlanNodeTree(planner::AbstractPlanNode* root) {
  if (!root)
    return false;

  // Clean all children subtrees
  auto children = root->GetChildren();
  for (auto child : children) {
    auto rv = CleanPlanNodeTree(child);
    assert(rv);
  }

  // Clean the root
  delete root;
  return true;
}

}  // namespace bridge
}  // namespace peloton

