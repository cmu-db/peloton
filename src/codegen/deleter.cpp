//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter.cpp
//
// Identification: src/codegen/deleter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/deleter.h"
#include "codegen/transaction_runtime.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

void Deleter::Init(concurrency::Transaction *txn, storage::DataTable *table,
                   executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn != nullptr && table != nullptr && executor_context != nullptr);
  txn_ = txn;
  table_ = table;
  executor_context_ = executor_context;
}

void Deleter::Delete(uint32_t tile_group_id, uint32_t tuple_offset,
                     executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);
  LOG_TRACE("Deleting tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            tile_group_id, tuple_offset, table_->GetName().c_str(),
            table_->GetDatabaseOid(), table_->GetOid());

  auto tile_group = table_->GetTileGroupById(tile_group_id);
  auto *tile_group_header = tile_group->GetHeader();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto is_owner = TransactionRuntime::IsOwner(*txn_, tile_group_header,
                                              tuple_offset);
  bool acquired_ownership = false;
  if (is_owner == false) {
    acquired_ownership = TransactionRuntime::AcquireOwnership(
        *txn_, tile_group_header, tuple_offset);
    if (!acquired_ownership)
      return;
  }

  ItemPointer new_location = table_->InsertEmptyVersion();
  if (new_location.IsNull()) {
    TransactionRuntime::YieldOwnership(*txn_, tile_group_header, tuple_offset);
    return;
  }

  ItemPointer old_location(tile_group_id, tuple_offset);
  txn_manager.PerformDelete(txn_, old_location, new_location);
  executor_context->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
