//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.cpp
//
// Identification: src/backend/bridge/ddl/ddl_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "backend/bridge/dml/mapper/dml_utils.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/logger.h"

#include "postgres.h"
#include "nodes/execnodes.h"
#include "common/fe_memutils.h"
#include "utils/rel.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

AbstractPlanState *
DMLUtils::BuildPlanState(AbstractPlanState *root,
                         PlanState *planstate,
                         bool left_child){
  // Base case
  if (planstate == nullptr) return root;

  AbstractPlanState *child_planstate = nullptr;

  auto planstate_type = nodeTag(planstate);
  switch (planstate_type) {

    case T_ModifyTableState:
      child_planstate = PrepareModifyTableState(
          reinterpret_cast<ModifyTableState *>(planstate));
      break;

    default:
      elog(ERROR, "unrecognized node type: %d", planstate_type);
      break;
  }

  // Base case
  if (child_planstate != nullptr) {
    if (root != nullptr) {
      if(left_child)
        root->left_tree = child_planstate;
      else
        root->right_tree = child_planstate;
    }
    else
      root = child_planstate;
  }

  // Recurse
  auto left_tree = outerPlanState(planstate);
  auto right_tree = innerPlanState(planstate);

  if(left_tree)
    BuildPlanState(child_planstate, left_tree, true);
  if(right_tree)
    BuildPlanState(child_planstate, right_tree, false);

  return root;
}

ModifyTablePlanState *
DMLUtils::PrepareModifyTableState(ModifyTableState *mt_plan_state) {

  ModifyTablePlanState *info = (ModifyTablePlanState*) palloc(sizeof(ModifyTablePlanState));
  info->type = mt_plan_state->ps.type;

  // Resolve result table
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;
  oid_t table_nattrs = result_relation_desc->rd_att->natts;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  info->operation = mt_plan_state->operation;
  info->database_oid = database_oid;
  info->table_oid = table_oid;
  info->table_nattrs = table_nattrs;

  // Should be only one sub plan which is a Result
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  // Child is a result node
  if (nodeTag(sub_planstate->plan) == T_Result) {
    LOG_TRACE("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState *>(sub_planstate);

    // We only handle single-constant-tuple for now,
    // i.e., ResultState should have no children/sub plans
    assert(outerPlanState(result_ps) == nullptr);

    //auto project_info = BuildProjectInfo(result_ps->ps.ps_ProjInfo, table_nattrs);
  }
  else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }

  return info;
}

/**
 * @brief preparing data
 * @param planstate
 */
AbstractPlanState *
DMLUtils::peloton_prepare_data(PlanState *planstate) {

  auto peloton_planstate = BuildPlanState(nullptr, planstate, false);

  return peloton_planstate;
}

}  // namespace bridge
}  // namespace peloton
