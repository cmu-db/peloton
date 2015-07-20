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

#include "backend/bridge/dml/mapper/mapper.h"

#include "nodes/print.h"
#include "nodes/pprint.h"
#include "utils/lsyscache.h"
#include "executor/executor.h"
#include "parser/parsetree.h"

void printPlanStateTree(const PlanState * planstate);

namespace peloton {
namespace bridge {

extern const ValueArray BuildParams(const ParamListInfo param_list);

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

  Plan *plan = plan_state->plan;
  planner::AbstractPlanNode *plan_node;

  LOG_INFO("planstate %d with #param", nodeTag(plan_state));

  ValueArray params = BuildParams(plan_state->state->es_param_list_info);

  switch (nodeTag(plan)) {
    case T_ModifyTable:
      plan_node = PlanTransformer::TransformModifyTable(
          reinterpret_cast<const ModifyTableState *>(plan_state), params);
      break;
    case T_SeqScan:
      plan_node = PlanTransformer::TransformSeqScan(
          reinterpret_cast<const SeqScanState*>(plan_state), params);
      break;
    case T_IndexScan:
      plan_node = PlanTransformer::TransformIndexScan(
          reinterpret_cast<const IndexScanState*>(plan_state), params);
      break;
    case T_IndexOnlyScan:
      plan_node = PlanTransformer::TransformIndexOnlyScan(
          reinterpret_cast<const IndexOnlyScanState*>(plan_state), params);
      break;
    case T_Limit:
      plan_node = PlanTransformer::TransformLimit(
          reinterpret_cast<const LimitState*>(plan_state));
      break;
    default: {
      plan_node = nullptr;
      LOG_ERROR("Unsupported Postgres Plan Tag: %u Plan : %p", nodeTag(plan),
                plan);
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

inline const ValueArray BuildParams(const ParamListInfo param_list) {
  ValueArray params;
  if (param_list != nullptr) {
    params.Reset(param_list->numParams);
    for (int i = 0; i < params.GetSize(); ++i) {
      ParamExternData param = param_list->params[i];
      params[i] = TupleTransformer::GetValue(param.value, param.ptype);
    }
  }
  LOG_INFO("Built param list of size %d", params.GetSize());
  return params;
}

}  // namespace bridge
}  // namespace peloton

