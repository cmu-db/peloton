//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_modify_table.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_modify_table.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/storage/data_table.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/delete_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// ModifyTable
//===--------------------------------------------------------------------===//

/**
 * @brief Convert ModifyTableState into AbstractPlan.
 * @return Pointer to the constructed AbstractPlan.
 *
 * Basically, it multiplexes into helper methods based on operation type.
 */
planner::AbstractPlan *PlanTransformer::TransformModifyTable(
    const ModifyTableState *mt_plan_state, const TransformOptions options) {
  ModifyTable *plan = (ModifyTable *)mt_plan_state->ps.plan;

  switch (plan->operation) {
    case CMD_INSERT:
      LOG_INFO("CMD_INSERT");
      return PlanTransformer::TransformInsert(mt_plan_state, options);
      break;

    case CMD_UPDATE:
      LOG_INFO("CMD_UPDATE");
      return PlanTransformer::TransformUpdate(mt_plan_state, options);
      break;

    case CMD_DELETE:
      LOG_INFO("CMD_DELETE");
      return PlanTransformer::TransformDelete(mt_plan_state, options);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", plan->operation);
      break;
  }

  return nullptr;
}

/**
 * @brief Convert ModifyTableState Insert case into AbstractPlan.
 * @return Pointer to the constructed AbstractPlan.
 */
planner::AbstractPlan *PlanTransformer::TransformInsert(
    const ModifyTableState *mt_plan_state, const TransformOptions options) {
  planner::AbstractPlan *plan_node = nullptr;

  /* Resolve result table */
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;
  oid_t table_nattrs = result_relation_desc->rd_att->natts;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Should be only one sub plan which is a Result  */
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  if (nodeTag(sub_planstate->plan) == T_Result) {  // Child is a result node
    LOG_TRACE("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState *>(sub_planstate);

    assert(outerPlanState(result_ps) ==
           nullptr); /* We only handle single-constant-tuple for now,
                 i.e., ResultState should have no children/sub plans */

    auto project_info =
        BuildProjectInfo(result_ps->ps.ps_ProjInfo, table_nattrs);

    plan_node = new planner::InsertPlan(database_oid, table_oid, project_info);

  } else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }

  return plan_node;
}

planner::AbstractPlan *PlanTransformer::TransformUpdate(
    const ModifyTableState *mt_plan_state, const TransformOptions options) {
  /*
   * NOTE:
   * In Postgres, the new tuple is returned by an underlying Scan node
   * (by means of non-trivial projections),
   * and the Postgres Update (ModifyTable) node merely replace the old tuple
   * with it.
   * In Peloton, we want to shift the responsibility of constructing the
   * new tuple to the Update node.
   * So, we peek and steal the projection info from our child,
   * but leave it to process the WHERE clause.
   */

  /* Should be only one sub plan which is a SeqScan */
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  /* Resolve result table */
  ResultRelInfo result_rel_info = mt_plan_state->resultRelInfo[0];
  Relation result_relation_desc = result_rel_info.ri_RelationDesc;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Get the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_TRACE("Update table : database oid %u table oid %u", database_oid,
            table_oid);

  /* Get the first sub plan state */
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];
  assert(sub_planstate);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  planner::AbstractPlan *plan_node = nullptr;

  auto child_tag = nodeTag(sub_planstate->plan);

  if (child_tag == T_SeqScan || child_tag == T_IndexScan ||
      child_tag == T_IndexOnlyScan ||
      child_tag == T_BitmapHeapScan) {  // Sub plan is a Scan of any type

    LOG_TRACE("Child of Update is %u \n", child_tag);
    // Extract the projection info from the underlying scan
    // and put it in our update node
    auto scan_state = reinterpret_cast<ScanState *>(sub_planstate);
    auto project_info =
        BuildProjectInfo(scan_state->ps.ps_ProjInfo, schema->GetColumnCount());

    plan_node = new planner::UpdatePlan(target_table, project_info);
    TransformOptions new_options = options;
    new_options.use_projInfo = false;
    plan_node->AddChild(TransformPlan(sub_planstate, new_options));
  } else {
    LOG_ERROR("Unsupported sub plan type of Update : %u \n", child_tag);
  }

  return plan_node;
}

/**
 * @brief Convert a Postgres ModifyTableState with DELETE operation
 * into a Peloton DeleteNode.
 * @return Pointer to the constructed AbstractPlan.
 *
 * Just like Peloton,
 * the delete plan state in Postgres simply deletes tuples
 * returned by a subplan (mostly Scan).
 * So we don't need to handle predicates locally .
 */
planner::AbstractPlan *PlanTransformer::TransformDelete(
    const ModifyTableState *mt_plan_state, const TransformOptions options) {
  // Grab Database ID and Table ID
  assert(mt_plan_state->resultRelInfo);  // Input must come from a subplan
  assert(mt_plan_state->mt_nplans ==
         1);  // Maybe relax later. I don't know when they can have >1 subplans.

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = mt_plan_state->resultRelInfo[0].ri_RelationDesc->rd_id;

  /* Grab the subplan -> child plan node */
  assert(mt_plan_state->mt_nplans == 1);
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  bool truncate = false;

  // Create the peloton plan node
  auto plan_node = new planner::DeletePlan(database_oid, table_oid, truncate);

  // Add child plan node(s)
  TransformOptions new_options = options;
  new_options.use_projInfo = false;
  plan_node->AddChild(TransformPlan(sub_planstate, new_options));

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
