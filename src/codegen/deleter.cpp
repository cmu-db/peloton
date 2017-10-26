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

void Deleter::Delete(uint32_t tile_group_id, uint32_t tuple_offset) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  LOG_TRACE("Deleting tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            tile_group_id, tuple_offset, table_->GetName().c_str(),
            table_->GetDatabaseOid(), table_->GetOid());

  // Punt to TransactionRuntime that does the heavy lifting
  auto result = TransactionRuntime::PerformDelete(*txn_, *table_, tile_group_id,
                                                  tuple_offset);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

}  // namespace codegen
}  // namespace peloton
