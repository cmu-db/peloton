/*
 * transformer.cpp
 *
 *  Copyright(c) 2015, CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */
extern "C" {
#include "nodes/pprint.h"
#include "utils/rel.h"
#include "executor/executor.h"
}
#include "backend/bridge/plan_transformer.h"
#include "backend/bridge/tuple_transformer.h"
#include "backend/bridge/bridge.h"
#include "backend/storage/data_table.h"
#include "backend/planner/insert_node.h"

extern "C" void printPlanStateTree(const PlanState * planstate);

namespace peloton {
namespace bridge {

PlanTransformer&
PlanTransformer::GetInstance() {
  static PlanTransformer planTransformer;
  return planTransformer;
}

void PlanTransformer::printPostgresPlanStateTree(
    const PlanState * planstate) const {
  printPlanStateTree(planstate);
}

/* @brief Convert Postgres PlanState into Peloton AbstractPlanNode.
 *
 *
 *
 */
PlanTransformer::AbstractPlanNodePtr PlanTransformer::transform(
    const PlanState * planstate) {
  Plan *plan = planstate->plan;
  AbstractPlanNodePtr planNode;

  /* 1. Plan Type */
  switch (nodeTag(plan)) {
    case T_ModifyTable:
      planNode = PlanTransformer::transformModifyTable(
          (ModifyTableState *) planstate);
      break;
    default:
      break;
  }

  return planNode;
}

PlanTransformer::AbstractPlanNodePtr PlanTransformer::transformModifyTable(
    const ModifyTableState *mtstate) {

  /* TODO: implement UPDATE, DELETE */
  /* TODO: Add logging */
  ModifyTable *plan = (ModifyTable *) mtstate->ps.plan;

  switch (plan->operation) {
    case CMD_INSERT:
      return PlanTransformer::transformInsert(mtstate);
      break;
    default:
      return PlanTransformer::AbstractPlanNodePtr();  // default ctor = nullptr
      break;
  }
}

PlanTransformer::AbstractPlanNodePtr PlanTransformer::transformInsert(
    const ModifyTableState *mtstate) {

  /* TODO: Add logging */

  /* Resolve result table */
  ResultRelInfo *resultRelInfo = mtstate->resultRelInfo;
  Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;

  /* Current Peloton only support plain insert statement
   * The number of subplan is exactly 1
   * TODO: can it be 0? */
  assert(mtstate->mt_nplans == 1);

  Oid database_oid = GetCurrentDatabaseOid();
  Oid table_oid = resultRelationDesc->rd_id;
  storage::DataTable *result_table =
      static_cast<storage::DataTable*>(catalog::Manager::GetInstance()
          .GetLocation(database_oid, table_oid));

  /* Resolve tuple */
  auto schema = result_table->GetSchema();

  PlanState *subplanstate = mtstate->mt_plans[0]; /* Should be only one which is a Result Plan */
  TupleTableSlot *planSlot;
  std::vector<storage::Tuple *> tuples;

  planSlot = ExecProcNode(subplanstate);
  assert(!TupIsNull(planSlot)); /* The tuple should not be null */
  storage::Tuple *tuple = TupleTransformer(planSlot, schema);
  tuples.push_back(tuple);

  return PlanTransformer::AbstractPlanNodePtr(
      new planner::InsertNode(result_table, tuples));
}

}
}
