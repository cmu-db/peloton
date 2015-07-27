/*-------------------------------------------------------------------------
 *
 * mapper_seq_scan.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_seq_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/seq_scan_node.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Seq Scan
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres SeqScanState into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlanNode.
 *
 * TODO: Can we also scan result from a child operator? (Non-base-table scan?)
 * We can't for now, but Postgres can.
 */
planner::AbstractPlanNode* PlanTransformer::TransformSeqScan(
    const SeqScanState* ss_plan_state) {
  assert(nodeTag(ss_plan_state) == T_SeqScanState);

  // Grab Database ID and Table ID
  assert(ss_plan_state->ss_currentRelation);  // Null if not a base table scan
  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = ss_plan_state->ss_currentRelation->rd_id;

  /* Grab the target table */
  storage::DataTable* target_table = static_cast<storage::DataTable*>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("SeqScan: database oid %u table oid %u", database_oid, table_oid);

  /*
   * Grab and transform the predicate.
   * And remember to free it at some point
   */
  expression::AbstractExpression* predicate = nullptr;

  if (ss_plan_state->ps.qual) {
    const ExprState* expr_state =
        reinterpret_cast<ExprState*>(ss_plan_state->ps.qual);
    predicate = ExprTransformer::TransformExpr(expr_state);
  }

  if (predicate) {
    LOG_INFO("Predicate :");
    std::cout << predicate->DebugInfo(" ");
  }

  /*
   * Grab and transform the projection information.
   * IMPORTANT: Projection may change the schema of the tile!
   */
  std::vector<oid_t> column_ids;

  auto schema = target_table->GetSchema();

  // We must relax the column count according to from tuple_desc
  // because user may type 'SELECT a, b, a, a'.
  std::unique_ptr<const planner::ProjectInfo> project_info(
      BuildProjectInfo(ss_plan_state->ps.ps_ProjInfo,
                        ss_plan_state->ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts));

  if(ss_plan_state->ps.ps_ProjInfo == nullptr){
    LOG_INFO("No projections (all pass through).");

    column_ids.resize(schema->GetColumnCount());
    std::iota(column_ids.begin(), column_ids.end(), 0);
  }
  else if(project_info->GetTargetList().size() > 0){
    LOG_WARN("Sorry we don't handle non-trivial projections for now.\n");

    column_ids.resize(schema->GetColumnCount());
    std::iota(column_ids.begin(), column_ids.end(), 0);
  }
  else {  // Pure direct map
    assert(project_info->GetTargetList().size() == 0);

    LOG_INFO("Pure direct map projection.\n");

    column_ids = BuildColumnListFromDirectMap(project_info->GetDirectMapList());

    assert(column_ids.size() == ss_plan_state->ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts);
  }

  assert(column_ids.size() > 0);

  /* Construct and return the Peloton plan node */
  auto plan_node =
      new planner::SeqScanNode(target_table, predicate, column_ids);
  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
