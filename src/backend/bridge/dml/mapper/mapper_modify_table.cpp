/*-------------------------------------------------------------------------
 *
 * mapper_modify_table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/plan/mapper_modify_table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/storage/data_table.h"
#include "backend/planner/insert_node.h"
#include "backend/planner/update_node.h"
#include "backend/planner/delete_node.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// ModifyTable
//===--------------------------------------------------------------------===//

extern std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformProjInfo(ProjectionInfo* proj_info, oid_t column_count);

extern std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformTargetList(List* target_list, oid_t column_count);

/**
 * @brief Convert ModifyTableState into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * Basically, it multiplexes into helper methods based on operation type.
 */
planner::AbstractPlanNode *PlanTransformer::TransformModifyTable(
    const ModifyTableState *mt_plan_state) {

  ModifyTable *plan = (ModifyTable *) mt_plan_state->ps.plan;

  switch (plan->operation) {
    case CMD_INSERT:
      LOG_INFO("CMD_INSERT");
      return PlanTransformer::TransformInsert(mt_plan_state);
      break;

    case CMD_UPDATE:
      LOG_INFO("CMD_UPDATE");
      return PlanTransformer::TransformUpdate(mt_plan_state);
      break;

    case CMD_DELETE:
      LOG_INFO("CMD_DELETE");
      return PlanTransformer::TransformDelete(mt_plan_state);
      break;

    default:
      LOG_ERROR("Unrecognized operation type : %u", plan->operation);
      break;
  }

  return nullptr;
}

/**
 * @brief Convert ModifyTableState Insert case into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *PlanTransformer::TransformInsert(
    const ModifyTableState *mt_plan_state) {

  planner::AbstractPlanNode *plan_node = nullptr;

  /* Resolve result table */
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Get the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
  .GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_INFO("Insert into: database oid %u table oid %u", database_oid, table_oid);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  /* Should be only one sub plan which is a Result  */
  assert(mt_plan_state->mt_nplans == 1);
  assert(mt_plan_state->mt_plans != nullptr);

  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  /*
   * We absorb the child of Insert to avoid
   * creating a child that returns just a tuple.
   * The cost is to make calls to AbstractExpression->Evaluate() here.
   */
  if (nodeTag(sub_planstate->plan) == T_Result) {  // Child is a result node
    LOG_INFO("Child of Insert is Result");
    auto result_ps = reinterpret_cast<ResultState*>(sub_planstate);

    assert(outerPlanState(result_ps) == nullptr); /* We only handle single-constant-tuple for now,
     i.e., ResultState should have no children/sub plans */

    auto projs = TransformTargetList(
        result_ps->ps.ps_ProjInfo->pi_targetlist, schema->GetColumnCount());

    plan_node = new planner::InsertNode(target_table, projs);

  } else {
    LOG_ERROR("Unsupported child type of Insert: %u",
              nodeTag(sub_planstate->plan));
  }


  return plan_node;
}

planner::AbstractPlanNode* PlanTransformer::TransformUpdate(
    const ModifyTableState* mt_plan_state) {

  /*
   * NOTE:
   * In Postgres, the new tuple is returned by an underlying Scan node
   * (by means of non-trivial projections),
   * and the Postgres Update (ModifyTable) node merely replace the old tuple with it.
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
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
  .GetTableWithOid(database_oid, table_oid));

  if (target_table == nullptr) {
    LOG_ERROR("Target table is not found : database oid %u table oid %u",
              database_oid, table_oid);
    return nullptr;
  }

  LOG_INFO("Update table : database oid %u table oid %u", database_oid,
           table_oid);

  /* Get the first sub plan state */
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];
  assert(sub_planstate);

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  planner::UpdateNode::ColumnExprs update_column_exprs;
  planner::AbstractPlanNode* plan_node = nullptr;

  if (nodeTag(sub_planstate->plan) == T_SeqScan) {  // Sub plan is SeqScan
    LOG_INFO("Child of Update is SeqScan \n");
    // Extract the non-trivial projection info from SeqScan
    // and put it in our update node
    auto seqscan_state = reinterpret_cast<SeqScanState*>(sub_planstate);

    //    update_column_exprs = TransformTargetList(
    //        seqscan_state->ps.ps_ProjInfo->pi_targetlist, schema->GetColumnCount());
    update_column_exprs = TransformProjInfo(seqscan_state->ps.ps_ProjInfo, schema->GetColumnCount());

    plan_node = new planner::UpdateNode(target_table, update_column_exprs);
    plan_node->AddChild(TransformPlan(sub_planstate));
  } else {
    LOG_ERROR("Unsupported sub plan type of Update : %u \n",
              nodeTag(sub_planstate->plan));
  }

  return plan_node;
}

/**
 * @brief Convert a Postgres ModifyTableState with DELETE operation
 * into a Peloton DeleteNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * Just like Peloton,
 * the delete plan state in Postgres simply deletes tuples
 *  returned by a subplan (mostly Scan).
 * So we don't need to handle predicates locally .
 */
planner::AbstractPlanNode* PlanTransformer::TransformDelete(
    const ModifyTableState* mt_plan_state) {

  // Grab Database ID and Table ID
  assert(mt_plan_state->resultRelInfo);  // Input must come from a subplan
  assert(mt_plan_state->mt_nplans == 1);  // Maybe relax later. I don't know when they can have >1 subplans.

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = mt_plan_state->resultRelInfo[0].ri_RelationDesc->rd_id;

  /* Grab the target table */
  storage::DataTable *target_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
  .GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("Delete from: database oid %u table oid %u", database_oid, table_oid);

  /* Grab the subplan -> child plan node */
  assert(mt_plan_state->mt_nplans == 1);
  PlanState *sub_planstate = mt_plan_state->mt_plans[0];

  bool truncate = false;

  // Create the peloton plan node
  auto plan_node = new planner::DeleteNode(target_table, truncate);

  // Add child plan node(s)
  plan_node->AddChild(TransformPlan(sub_planstate));

  return plan_node;
}

} // namespace bridge
} // namespace peloton

