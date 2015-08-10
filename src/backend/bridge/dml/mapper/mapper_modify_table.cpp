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

#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// ModifyTable
//===--------------------------------------------------------------------===//

/**
 * @brief Convert ModifyTable into AbstractPlan.
 * @return Pointer to the constructed AbstractPlan.
 *
 * Basically, it multiplexes into helper methods based on operation type.
 */
planner::AbstractPlan *PlanTransformer::TransformModifyTable(
    planner::ModifyTablePlanState *mt_planstate,
    const TransformOptions options) {

  switch (mt_planstate->operation) {
    case CMD_INSERT:
      LOG_INFO("CMD_INSERT");
      return PlanTransformer::TransformInsert(mt_planstate, options);
      break;

    case CMD_UPDATE:
      LOG_INFO("CMD_UPDATE");
      return PlanTransformer::TransformUpdate(mt_planstate, options);
      break;

    case CMD_DELETE:
      LOG_INFO("CMD_DELETE");
      return PlanTransformer::TransformDelete(mt_planstate, options);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", mt_planstate->operation);
      break;
  }

  return nullptr;
}

/**
 * @brief Convert ModifyTable Insert case into AbstractPlan.
 * @return Pointer to the constructed AbstractPlan.
 */
planner::AbstractPlan *PlanTransformer::TransformInsert(
    planner::ModifyTablePlanState *mt_planstate,
    const TransformOptions options) {
  planner::AbstractPlan *plan_node = nullptr;

  /* Resolve result table */

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = mt_planstate->table_oid;

  /* Get the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_TRACE("Insert into: database oid %u table oid %u", database_oid,
            table_oid);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  /* Should be only one sub plan which is a Result  */
  assert(mt_planstate->mt_nplans == 1);
  assert(mt_planstate->mt_plans != nullptr);

  planner::AbstractPlanState *sub_planstate = mt_planstate->mt_plans[0];

  if (nodeTag(sub_planstate) == T_ResultState) {  // Child is a result node
    LOG_TRACE("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState *>(sub_planstate);

    assert(outerPlanState(result_ps) ==
           nullptr); /* We only handle single-constant-tuple for now,
                 i.e., ResultState should have no children/sub plans */

    auto project_info =
        BuildProjectInfo(result_ps->ps.ps_ProjInfo, schema->GetColumnCount());

    plan_node = new planner::InsertPlan(target_table, project_info);

  } else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate));
  }

  return plan_node;
}

planner::AbstractPlan *PlanTransformer::TransformUpdate(
    planner::ModifyTablePlanState *mt_planstate,
    const TransformOptions options) {
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
  assert(mt_planstate->mt_nplans == 1);
  assert(mt_planstate->mt_plans != nullptr);

  /* Resolve result table */
  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = mt_planstate->table_oid;

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
  planner::AbstractPlanState *sub_planstate = mt_planstate->mt_plans[0];
  assert(sub_planstate);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  planner::AbstractPlan *plan_node = nullptr;

  auto child_tag = nodeTag(sub_planstate);

  if (child_tag == T_SeqScanState ||
      child_tag == T_IndexScanState ||
      child_tag == T_IndexOnlyScanState ||
      child_tag == T_BitmapHeapScanState ) {  // Sub plan is a Scan of any type

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
 * @brief Convert a Postgres ModifyTable with DELETE operation
 * into a Peloton DeleteNode.
 * @return Pointer to the constructed AbstractPlan.
 *
 * Just like Peloton,
 * the delete plan state in Postgres simply deletes tuples
 * returned by a subplan (mostly Scan).
 * So we don't need to handle predicates locally .
 */
planner::AbstractPlan *PlanTransformer::TransformDelete(
    planner::ModifyTablePlanState *mt_planstate,
    const TransformOptions options) {
  // Grab Database ID and Table ID
  assert(mt_planstate->table_oid);  // Input must come from a subplan
  assert(mt_planstate->mt_nplans == 1);  // Maybe relax later. I don't know when they can have >1 subplans.

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = mt_planstate->table_oid;

  /* Grab the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("Delete from: database oid %u table oid %u", database_oid,
           table_oid);

  /* Grab the subplan -> child plan node */
  assert(mt_planstate->mt_nplans == 1);
  planner::AbstractPlanState *sub_planstate = mt_planstate->mt_plans[0];

  bool truncate = false;

  // Create the peloton plan node
  auto plan_node = new planner::DeletePlan(target_table, truncate);

  // Add child plan node(s)
  TransformOptions new_options = options;
  new_options.use_projInfo = false;
  plan_node->AddChild(TransformPlan(sub_planstate, new_options));

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
