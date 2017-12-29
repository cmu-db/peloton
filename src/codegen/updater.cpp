//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater.cpp
//
// Identification: src/codegen/updater.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updater.h"
#include "codegen/transaction_runtime.h"
#include "common/container_tuple.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile_group.h"
#include "storage/tile.h"
#include "storage/tuple.h"
#include "type/abstract_pool.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

void Updater::Init(storage::DataTable *table,
                   executor::ExecutorContext *executor_context,
                   Target *target_vector, uint32_t target_vector_size) {
  PL_ASSERT(table != nullptr && executor_context != nullptr&&
            target_vector != nullptr);
  table_ = table;
  executor_context_ = executor_context;
  // Target list is kept since it is required at a new version update
  target_list_ =
      new TargetList(target_vector, target_vector + target_vector_size);
}

char *Updater::GetDataPtr(uint32_t tile_group_id, uint32_t tuple_offset) {
  auto tile_group = table_->GetTileGroupById(tile_group_id);

  // Get the tile offset assuming that it is still in a tuple format
  oid_t tile_offset, tile_column_offset;
  tile_group->LocateTileAndColumn(0, tile_offset, tile_column_offset);
  tile_ = tile_group->GetTileReference(tile_offset);
  return tile_->GetTupleLocation(tuple_offset);
}

char *Updater::Prepare(uint32_t tile_group_id, uint32_t tuple_offset) {
  PL_ASSERT(table_ != nullptr && executor_context_ != nullptr);
  auto *txn = executor_context_->GetTransaction();
  auto tile_group = table_->GetTileGroupById(tile_group_id).get();
  auto *tile_group_header = tile_group->GetHeader();
  old_location_.block = tile_group_id;
  old_location_.offset = tuple_offset;

  // If I am the owner, update in-place
  is_owner_ = TransactionRuntime::IsOwner(*txn, tile_group_header,
                                          tuple_offset);
  if (is_owner_ == true)
    return GetDataPtr(tile_group_id, tuple_offset);

  // If not the owner, acquire ownership and build a new version tuple
  acquired_ownership_ = TransactionRuntime::AcquireOwnership(*txn,
      tile_group_header, tuple_offset);
  if (acquired_ownership_ == false)
    return nullptr;

  new_location_ = table_->AcquireVersion();
  return GetDataPtr(new_location_.block, new_location_.offset);
}

char *Updater::PreparePK(uint32_t tile_group_id, uint32_t tuple_offset) {
  PL_ASSERT(table_ != nullptr && executor_context_ != nullptr);
  auto *txn = executor_context_->GetTransaction();
  auto tile_group = table_->GetTileGroupById(tile_group_id).get();
  auto *tile_group_header = tile_group->GetHeader();

  // Check ownership
  is_owner_ = TransactionRuntime::IsOwner(*txn, tile_group_header,
                                          tuple_offset);
  acquired_ownership_ = false;
  if (is_owner_ == false) {
    acquired_ownership_ = TransactionRuntime::AcquireOwnership(
        *txn, tile_group_header, tuple_offset);
    if (acquired_ownership_ == false)
      return nullptr;
  }

  // Delete the old tuple
  ItemPointer old_location(tile_group_id, tuple_offset);
  ItemPointer empty_location = table_->InsertEmptyVersion();
  if (empty_location.IsNull() == true && acquired_ownership_ == true) {
    TransactionRuntime::YieldOwnership(*txn, tile_group_header,
                                       tuple_offset);
    return nullptr;
  }
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.PerformDelete(txn, old_location, empty_location);

  // Get the tuple data pointer for a new version
  new_location_ = table_->GetEmptyTupleSlot(nullptr);
  return GetDataPtr(new_location_.block, new_location_.offset);
}

peloton::type::AbstractPool *Updater::GetPool() {
  // This should be called after Prepare() or PreparePK()
  PL_ASSERT(tile_);
  return tile_->GetPool();
}

void Updater::Update() {
  PL_ASSERT(table_ != nullptr && executor_context_ != nullptr);
  LOG_TRACE("Updating tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            old_location_.block, old_location_.offset,
            table_->GetName().c_str(), table_->GetDatabaseOid(),
            table_->GetOid());
  auto *txn = executor_context_->GetTransaction();
  auto tile_group = table_->GetTileGroupById(old_location_.block).get();
  auto *tile_group_header = tile_group->GetHeader();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Either update in-place
  if (is_owner_ == true) {
    txn_manager.PerformUpdate(txn, old_location_);
    executor_context_->num_processed++;
    return;
  }

  // Or, update with a new version
  ContainerTuple<storage::TileGroup> new_tuple(
    table_->GetTileGroupById(new_location_.block).get(), new_location_.offset);
  ItemPointer *indirection =
      tile_group_header->GetIndirection(old_location_.offset);
  auto result = table_->InstallVersion(&new_tuple, target_list_, txn,
                                       indirection);
  if (result == false) {
    TransactionRuntime::YieldOwnership(*txn, tile_group_header,
                                       old_location_.offset);
    return;
  }
  txn_manager.PerformUpdate(txn, old_location_, new_location_);
  executor_context_->num_processed++;
}

void Updater::UpdatePK() {
  PL_ASSERT(table_ != nullptr && executor_context_ != nullptr);
  LOG_TRACE("Updating tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            old_location_.block, old_location_.offset,
            table_->GetName().c_str(), table_->GetDatabaseOid(),
            table_->GetOid());
  auto *txn = executor_context_->GetTransaction();
  auto tile_group = table_->GetTileGroupById(new_location_.block).get();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Insert a new tuple
  ContainerTuple<storage::TileGroup> tuple(tile_group, new_location_.offset);
  ItemPointer *index_entry_ptr = nullptr;
  bool result = table_->InsertTuple(&tuple, new_location_, txn,
                                    &index_entry_ptr);
  if (result == false) {
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }
  txn_manager.PerformInsert(txn, new_location_, index_entry_ptr);
  executor_context_->num_processed++;
}

void Updater::TearDown() {
  // Updater object does not destruct its own data structures
  tile_.reset();
  delete target_list_;
}

}  // namespace codegen
}  // namespace peloton
