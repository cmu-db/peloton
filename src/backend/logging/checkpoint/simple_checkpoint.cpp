/*-------------------------------------------------------------------------
 *
 * simple_checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint/simple_checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/executors.h"
#include "backend/executor/executor_context.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/catalog/manager.h"

#include "backend/common/logger.h"
#include "backend/common/types.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

void SimpleCheckpoint::DoCheckpoint() {
  // build executor context
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  assert(txn);

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");
  auto executor_context = new executor::ExecutorContext(
      txn, bridge::PlanTransformer::BuildParams(nullptr));

  // TODO Grab Database ID and Table ID
  Oid database_oid = 0;
  Oid table_oid = 0;

  /* Grab the target table */
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Manager::GetInstance().GetTableWithOid(database_oid, table_oid));

  assert(target_table);
  LOG_INFO("SeqScan: database oid %u table oid %u: %s", database_oid, table_oid,
           target_table->GetName().c_str());

  expression::AbstractExpression *predicate = nullptr;
  std::vector<oid_t> column_ids;
  column_ids.resize(target_table->GetSchema()->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  /* Construct the Peloton plan node */
  auto scan_plan_node =
      new planner::SeqScanPlan(target_table, predicate, column_ids);

  executor::AbstractExecutor *scan_executor = nullptr;
  scan_executor =
      new executor::SeqScanExecutor(scan_plan_node, executor_context);

  LOG_TRACE("Initializing the executor tree");

  // Initialize the seq scan executor
  // bool init_failure = false;
  auto status = scan_executor->Init();

  // Abort and cleanup
  if (status == false) {
    // init_failure = true;
    txn->SetResult(Result::RESULT_FAILURE);
    goto cleanup;
  }
  LOG_TRACE("Running the seq scan executor");

  // Execute seq scan until we get result tiles
  for (;;) {
    status = scan_executor->Execute();

    // Stop
    if (status == false) {
      break;
    }

    std::unique_ptr<executor::LogicalTile> logical_tile(
        scan_executor->GetOutput());

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          logical_tile.get(), tuple_id);
      // Construct log records
    }
  }

cleanup:
  // Do cleanup
  ;
}

}  // namespace logging
}  // namespace peloton
