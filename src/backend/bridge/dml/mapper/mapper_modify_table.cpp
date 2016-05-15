//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_modify_table.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_modify_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/catalog/manager.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/delete_plan.h"
#include "backend/storage/data_table.h"

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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformModifyTable(
    const ModifyTablePlanState *mt_plan_state, const TransformOptions options) {
  auto operation = mt_plan_state->operation;

  switch (operation) {
    case CMD_INSERT:
      LOG_TRACE("CMD_INSERT");
      return std::move(
          PlanTransformer::TransformInsert(mt_plan_state, options));
      break;

    case CMD_UPDATE:
      LOG_TRACE("CMD_UPDATE");
      return PlanTransformer::TransformUpdate(mt_plan_state, options);
      break;

    case CMD_DELETE:
      LOG_TRACE("CMD_DELETE");
      return PlanTransformer::TransformDelete(mt_plan_state, options);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", operation);
      break;
  }

  return nullptr;
}

/**
 * @brief Convert ModifyTableState Insert case into AbstractPlan.
 * @return Pointer to the constructed AbstractPlan.
 */
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformInsert(
    const ModifyTablePlanState *mt_plan_state,
    UNUSED_ATTRIBUTE const TransformOptions options) {
  Oid database_oid = mt_plan_state->database_oid;
  Oid table_oid = mt_plan_state->table_oid;

  /* Get the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_TRACE("Insert into: database oid %u table oid %u: %s", database_oid,
           table_oid, target_table->GetName().c_str());

  AbstractPlanState *sub_planstate = mt_plan_state->mt_plans[0];
  ResultPlanState *result_planstate = (ResultPlanState *)sub_planstate;

  std::unique_ptr<const planner::ProjectInfo> project_info{
      BuildProjectInfo(result_planstate->proj)};

  std::unique_ptr<planner::AbstractPlan> plan_node(
      new planner::InsertPlan(target_table, std::move(project_info)));

  return plan_node;
}

std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformUpdate(
    const ModifyTablePlanState *mt_plan_state, const TransformOptions options) {
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

  Oid database_oid = mt_plan_state->database_oid;
  Oid table_oid = mt_plan_state->table_oid;

  // Get the target table
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_TRACE("Update table : database oid %u table oid %u: %s", database_oid,
           table_oid, target_table->GetName().c_str());

  // Child must be a scan node
  auto sub_planstate = (AbstractScanPlanState *)mt_plan_state->mt_plans[0];

  std::unique_ptr<const planner::ProjectInfo> project_info{
      BuildProjectInfo(sub_planstate->proj)};

  std::unique_ptr<planner::AbstractPlan> plan_node(
      new planner::UpdatePlan(target_table, std::move(project_info)));

  TransformOptions new_options = options;
  new_options.use_projInfo = false;

  plan_node->AddChild(std::move(TransformPlan(sub_planstate, new_options)));

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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformDelete(
    const ModifyTablePlanState *mt_plan_state, const TransformOptions options) {
  // Grab Database ID and Table ID

  Oid database_oid = mt_plan_state->database_oid;
  Oid table_oid = mt_plan_state->table_oid;

  /* Get the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_TRACE("Delete :: database oid %u table oid %u: %s", database_oid,
           table_oid, target_table->GetName().c_str());

  // Create the peloton plan node
  bool truncate = false;
  std::unique_ptr<planner::AbstractPlan> plan_node(
      new planner::DeletePlan(target_table, truncate));

  // Add child plan node(s)
  TransformOptions new_options = options;
  new_options.use_projInfo = false;

  auto sub_planstate = mt_plan_state->mt_plans[0];
  auto child_plan_node = TransformPlan(sub_planstate, new_options);

  plan_node.get()->AddChild(std::move(child_plan_node));

  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
