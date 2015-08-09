//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_seq_scan.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_seq_scan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../../../planner/seq_scan_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Seq Scan
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres SeqScanState into a Peloton SeqScanNode.
 * @return Pointer to the constructed AbstractPlan.
 *
 * TODO: Can we also scan result from a child operator? (Non-base-table scan?)
 */
planner::AbstractPlan *PlanTransformer::TransformSeqScan(
    const SeqScanState *ss_plan_state, const TransformOptions options) {
  assert(nodeTag(ss_plan_state) == T_SeqScanState);

  // Grab Database ID and Table ID
  assert(ss_plan_state->ss_currentRelation);  // Null if not a base table scan
  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  Oid table_oid = ss_plan_state->ss_currentRelation->rd_id;

  /* Grab the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("SeqScan: database oid %u table oid %u", database_oid, table_oid);

  /**
   * SeqScan only needs the "generic" settings, so grab it.
   */
  planner::AbstractPlan *parent = nullptr;
  expression::AbstractExpression *predicate = nullptr;
  std::vector<oid_t> column_ids;

  GetGenericInfoFromScanState(parent, predicate, column_ids, ss_plan_state,
                              options.use_projInfo);

  if (column_ids.empty()) {
    column_ids.resize(target_table->GetSchema()->GetColumnCount());
    std::iota(column_ids.begin(), column_ids.end(), 0);
  }

  /* Construct and return the Peloton plan node */
  auto scan_node =
      new planner::SeqScanPlan(target_table, predicate, column_ids);

  planner::AbstractPlan *rv = nullptr;

  /* Check whether a parent is presented, connect with the scan node if yes */
  if (parent) {
    parent->AddChild(scan_node);
    rv = parent;
  } else {
    rv = scan_node;
  }

  return rv;
}

}  // namespace bridge
}  // namespace peloton
