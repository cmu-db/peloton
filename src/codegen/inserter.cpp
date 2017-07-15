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
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile.h"

namespace peloton {
namespace codegen {

void Inserter::Init(concurrency::Transaction *txn, storage::DataTable *table,
                    executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn && table && executor_context);
  txn_ = txn;
  table_ = table;
  executor_context_ = executor_context;
}

char *Inserter::ReserveTupleStorage() {
  location_ = table_->GetEmptyTupleSlot(nullptr);

  // Get tuple storage area
  auto tile_group = table_->GetTileGroupById(location_.block);
  tile_ = tile_group->GetTileReference(0);
  return tile_->GetTupleLocation(location_.offset);
}

peloton::type::AbstractPool *Inserter::GetPool() {
  PL_ASSERT(tile_);
  return tile_->GetPool();
}

void Inserter::InsertReserved() {
  PL_ASSERT(txn_ && table_ && executor_context_ && tile_);

  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::WrapTiles({tile_}));
  expression::ContainerTuple<executor::LogicalTile> tuple(logical_tile.get(),
                                                          location_.offset);
  auto result = TransactionRuntime::PerformInsert(*txn_, *table_, &tuple,
                                                  location_);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

void Inserter::Insert(const storage::Tuple *tuple) {
  PL_ASSERT(txn_ && table_ && executor_context_);

  auto result = TransactionRuntime::PerformInsert(*txn_, *table_, tuple);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

}  // namespace codegen
}  // namespace peloton
