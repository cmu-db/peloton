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

#include "nodes/pprint.h"
#include "utils/rel.h"
#include "bridge/bridge.h"
#include "executor/executor.h"

#include "backend/bridge/plan_transformer.h"
#include "backend/bridge/tuple_transformer.h"
#include "backend/storage/data_table.h"
#include "backend/planner/insert_node.h"

void printPlanStateTree(const PlanState * planstate);

namespace peloton {
namespace bridge {

/**
 * @brief Get an instance of the plan transformer.
 * @return The instance.
 */
PlanTransformer& PlanTransformer::GetInstance() {
  static PlanTransformer planTransformer;
  return planTransformer;
}

/**
 * @brief Pretty print the plan state tree.
 * @return none.
 */
void PlanTransformer::PrintPlanState(const PlanState *plan_state) const {
  printPlanStateTree(plan_state);
}

/**
 * @brief Convert Postgres PlanState into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *PlanTransformer::TransformPlan(const PlanState *plan_state) {

  Plan *plan = plan_state->plan;
  planner::AbstractPlanNode *plan_node;

  switch (nodeTag(plan)) {
    case T_ModifyTable:
      plan_node = PlanTransformer::TransformModifyTable((ModifyTableState *) plan_state);
      break;

    default:
      plan_node = nullptr;
      break;
  }

  return plan_node;
}

/**
 * @brief Convert ModifyTableState into AbstractPlanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 */
planner::AbstractPlanNode *PlanTransformer::TransformModifyTable(
    const ModifyTableState *mt_plan_state) {

  /* TODO: handle UPDATE, DELETE */
  /* TODO: Add logging */
  ModifyTable *plan = (ModifyTable *) mt_plan_state->ps.plan;

  switch (plan->operation) {
    case CMD_INSERT:
      return PlanTransformer::TransformInsert(mt_plan_state);
      break;

    default:
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

  /* Resolve result table */
  ResultRelInfo *result_rel_info = mt_plan_state->resultRelInfo;
  Relation result_relation_desc = result_rel_info->ri_RelationDesc;

  /* Currently, we only support plain insert statement.
   * So, the number of subplan must be exactly 1.
   * TODO: can it be 0? */
  assert(mt_plan_state->mt_nplans == 1);

  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = result_relation_desc->rd_id;

  /* Get the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable*>(catalog::Manager::GetInstance().
      GetLocation(database_oid, table_oid));

  /* Get the tuple schema */
  auto schema = target_table->GetSchema();

  /* Should be only one which is a Result Plan */
  PlanState *subplan_state = mt_plan_state->mt_plans[0];
  TupleTableSlot *plan_slot;
  std::vector<storage::Tuple *> tuples;

  /*
  plan_slot = ExecProcNode(subplan_state);
  assert(!TupIsNull(plan_slot)); // The tuple should not be null

  auto tuple = TupleTransformer(plan_slot, schema);
  tuples.push_back(tuple);
  */

  auto plan_node = new planner::InsertNode(target_table, tuples);

  return plan_node;
}

} // namespace bridge
} // namespace peloton
