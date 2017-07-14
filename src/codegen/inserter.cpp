//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter.cpp
//
// Identification: src/codegen/inserter.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/inserter.h"
#include "codegen/transaction_runtime.h"
#include "common/item_pointer.h"
#include "executor/executor_context.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile.h"
#include "storage/tuple.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

void Inserter::Init(concurrency::Transaction *txn, storage::DataTable *table,
                    executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn != nullptr && table != nullptr && executor_context != nullptr);
  txn_ = txn;
  table_ = table;
  executor_context_ = executor_context;
}

void Inserter::CreateTuple() {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);
}

char *Inserter::ReserveTupleStorage() {
  location_ = table_->GetEmptyTupleSlot(nullptr);
  auto tile_group = table_->GetTileGroupById(location_.block);
  tile_ = tile_group->GetTile(0);
  return tile_->GetTupleLocation(location_.offset);
}

peloton::type::AbstractPool *Inserter::GetPool() {
  PL_ASSERT(tile_ != nullptr);
  return tile_->GetPool();
}

void Inserter::InsertReserved() {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr && executor_context_ != nullptr);

  auto result = TransactionRuntime::PerformInsert(*txn_, *table_, tuple_.get(),
                                                  location_);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

void Inserter::Insert(const storage::Tuple *tuple) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr && executor_context_ != nullptr);

  auto result = TransactionRuntime::PerformInsert(*txn_, *table_, tuple);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

void Inserter::Destroy() {
  tuple_.reset(nullptr);
}

}  // namespace codegen
}  // namespace peloton
