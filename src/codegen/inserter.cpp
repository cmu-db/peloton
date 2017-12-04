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
#include "common/item_pointer.h"
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
  filter_ = nullptr;
}

void Inserter::CreateFilter() {
  filter_ = new std::unordered_set<ItemPointer, ItemPointerHasher,
                                   ItemPointerComparator>;
}

bool Inserter::IsEligible(oid_t tile_group_id, oid_t tuple_offset) {
  PL_ASSERT(filter_);
  ItemPointer location(tile_group_id, tuple_offset);
  auto it = filter_->find(location);
  return (it == filter_->end());
}

char *Inserter::ReserveTupleStorage() {
  location_ = table_->GetEmptyTupleSlot(nullptr);

  // Get the tile offset assuming that it is in a tuple format
  auto tile_group = table_->GetTileGroupById(location_.block);
  oid_t tile_offset, tile_column_offset;
  tile_group->LocateTileAndColumn(0, tile_offset, tile_column_offset);
  tile_ = tile_group->GetTileReference(tile_offset);
  return tile_->GetTupleLocation(location_.offset);
}

peloton::type::AbstractPool *Inserter::GetPool() {
  // This should be called after RerserveTupleStorage()
  PL_ASSERT(tile_);
  return tile_->GetPool();
}

void Inserter::InsertReserved() {
  PL_ASSERT(table_ && executor_context_ && tile_ && filter_);
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

  // Keep this in the cache so that we can filter this
  filter_->insert(location_);

  // the tile pointer is there for an insertion, so we release it at this moment
  tile_.reset();
}

void Inserter::Insert(const storage::Tuple *tuple) {
  PL_ASSERT(table_ && executor_context_);
  auto *txn = executor_context_->GetTransaction();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  ItemPointer *index_entry_ptr = nullptr;
  ItemPointer location = table_->InsertTuple(tuple, txn, &index_entry_ptr);
  if (location.block == INVALID_OID) {
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }
  txn_manager.PerformInsert(txn, location, index_entry_ptr);
  executor_context_->num_processed++;
}

void Inserter::TearDown() {
  if (filter_ != nullptr)
    delete filter_;
}

}  // namespace codegen
}  // namespace peloton
