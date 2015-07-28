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

  /**
   * SeqScan only needs the "generic" settings, so grab it.
   */
  planner::AbstractPlanNode* parent = nullptr;
  expression::AbstractExpression* predicate = nullptr;
  std::vector<oid_t> column_ids;

  TransformGenericScanInfo(parent, predicate, column_ids, ss_plan_state);

  /* TODO: test whether parent is presented, connect with the scan node */

  /* Construct and return the Peloton plan node */
  auto plan_node =
      new planner::SeqScanNode(target_table, predicate, column_ids);
  return plan_node;
}

}  // namespace bridge
}  // namespace peloton
