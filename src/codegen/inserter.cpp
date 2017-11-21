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
#include "common/container_tuple.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile.h"

namespace peloton {
namespace codegen {

void Inserter::Init(storage::DataTable *table,
                    executor::ExecutorContext *executor_context) {
  PL_ASSERT(table && executor_context);
  table_ = table;
  executor_context_ = executor_context;
}

char *Inserter::AllocateTupleStorage() {
  location_ = table_->GetEmptyTupleSlot(nullptr);

  // Get the tile offset assuming that it is in a tuple format
  auto tile_group = table_->GetTileGroupById(location_.block);
  oid_t tile_offset, tile_column_offset;
  tile_group->LocateTileAndColumn(0, tile_offset, tile_column_offset);
  tile_ = tile_group->GetTileReference(tile_offset);
  return tile_->GetTupleLocation(location_.offset);
}

peloton::type::AbstractPool *Inserter::GetPool() {
  // This should be called after AllocateTupleStorage()
  PL_ASSERT(tile_);
  return tile_->GetPool();
}

void Inserter::Insert() {
  PL_ASSERT(table_ && executor_context_ && tile_);
  auto *txn = executor_context_->GetTransaction();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  ContainerTuple<storage::TileGroup> tuple(
      table_->GetTileGroupById(location_.block).get(), location_.offset);
  ItemPointer *index_entry_ptr = nullptr;
  bool result = table_->InsertTuple(&tuple, location_, txn, &index_entry_ptr);
  if (result == false) {
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }
  txn_manager.PerformInsert(txn, location_, index_entry_ptr);
  executor_context_->num_processed++;
}

void Inserter::TearDown() {
  // Updater object does not destruct its own data structures
  tile_.reset();
}

}  // namespace codegen
}  // namespace peloton
