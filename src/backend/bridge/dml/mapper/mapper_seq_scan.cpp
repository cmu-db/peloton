//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mapper_seq_scan.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_seq_scan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/catalog/manager.h"

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
std::unique_ptr<planner::AbstractPlan> PlanTransformer::TransformSeqScan(
    const SeqScanPlanState *ss_plan_state, const TransformOptions options) {
  assert(nodeTag(ss_plan_state) == T_SeqScanState);

  // Grab Database ID and Table ID
  Oid database_oid = ss_plan_state->database_oid;
  Oid table_oid = ss_plan_state->table_oid;

  /* Grab the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_TRACE("SeqScan: database oid %u table oid %u: %s", database_oid, table_oid,
           target_table->GetName().c_str());

  /**
   * SeqScan only needs the "generic" settings, so grab it.
   */
  planner::AbstractPlan *parent = nullptr;
  expression::AbstractExpression *predicate = nullptr;
  std::vector<oid_t> column_ids;

  GetGenericInfoFromScanState(parent, predicate, column_ids, ss_plan_state,
                              options.use_projInfo);

  if (column_ids.empty()) {
    column_ids.resize(ss_plan_state->table_nattrs);
    std::iota(column_ids.begin(), column_ids.end(), 0);
  }

  /* Construct and return the Peloton plan node */
  std::unique_ptr<planner::SeqScanPlan> scan_node(
      new planner::SeqScanPlan(target_table, predicate, column_ids));

  std::unique_ptr<planner::AbstractPlan> rv;

  /* Check whether a parent is presented, connect with the scan node if yes */
  if (parent) {
    parent->AddChild(std::move(scan_node));
    rv = std::unique_ptr<planner::AbstractPlan>(parent);
  } else {
    rv = std::unique_ptr<planner::AbstractPlan>(scan_node.release());
  }

  return rv;
}

}  // namespace bridge
}  // namespace peloton
