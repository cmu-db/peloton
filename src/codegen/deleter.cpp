//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter.cpp
//
// Identification: src/codegen/deleter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/deleter.h"

#include "codegen/transaction_runtime.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

Deleter::Deleter(storage::DataTable *table,
                 executor::ExecutorContext *executor_context)
    : table_(table), executor_context_(executor_context) {
  PELOTON_ASSERT(table != nullptr && executor_context != nullptr);
}

void Deleter::Init(Deleter &deleter, storage::DataTable *table,
                   executor::ExecutorContext *executor_context) {
  new (&deleter) Deleter(table, executor_context);
}

void Deleter::Delete(uint32_t tile_group_id, uint32_t tuple_offset) {
  LOG_TRACE("Deleting tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            tile_group_id, tuple_offset, table_->GetName().c_str(),
            table_->GetDatabaseOid(), table_->GetOid());

  auto *txn = executor_context_->GetTransaction();
  auto tile_group = table_->GetTileGroupById(tile_group_id);
  auto *tile_group_header = tile_group->GetHeader();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  bool is_owner = txn_manager.IsOwner(txn, tile_group_header, tuple_offset);
  bool is_written = txn_manager.IsWritten(txn, tile_group_header, tuple_offset);

  ItemPointer old_location{tile_group_id, tuple_offset};

  // Did the current transaction create this version we're deleting? If so, we
  // can perform the deletion without inserting a new empty version.

  if (is_owner && is_written) {
    LOG_TRACE("The current transaction is the owner of the tuple");
    txn_manager.PerformDelete(txn, old_location);
    executor_context_->num_processed++;
    return;
  }

  // We didn't create this version. In order to perform the delete, we need to
  // acquire ownership of the version. Let's check if we can do so.

  bool is_ownable =
      is_owner || txn_manager.IsOwnable(txn, tile_group_header, tuple_offset);
  if (!is_ownable) {
    // Version is not own-able. The transaction should be aborted as we cannot
    // update the latest version.
    LOG_TRACE("Tuple [%u-%u] isn't own-able. Failing transaction.",
              tile_group_id, tuple_offset);
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }

  // Version is own-able. Let's grab ownership of the version.

  bool acquired_ownership =
      is_owner ||
      txn_manager.AcquireOwnership(txn, tile_group_header, tuple_offset);
  if (!acquired_ownership) {
    LOG_TRACE(
        "Failed acquiring ownership of tuple [%u-%u]. Failing transaction.",
        tile_group_id, tuple_offset);
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }

  // We've acquired ownership of the latest version, and it isn't locked by any
  // other threads. Now, let's insert an empty version.

  ItemPointer new_location = table_->InsertEmptyVersion();

  // Insertion into the table may fail. PerformDelete() should not be called if
  // the insertion fails. At this point, we've acquired a write lock on the
  // version, but since it is not in the write set (since we haven't yet put it
  // into the write set), the acquired lock can't be released when the
  // transaction is aborted. The YieldOwnership() function helps us release the
  // acquired write lock.

  if (new_location.IsNull()) {
    LOG_TRACE("Failed to insert new tuple. Failing transaction.");
    if (!is_owner) {
      // If ownership was acquired, we release it here, thus releasing the
      // write lock.
      txn_manager.YieldOwnership(txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }

  // All is well
  txn_manager.PerformDelete(txn, old_location, new_location);
  executor_context_->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
