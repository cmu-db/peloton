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

#include "concurrency/transaction_manager_factory.h"

#include "catalog/manager.h"
#include "codegen/updater.h"
#include "codegen/transaction_runtime.h"
#include "common/container_tuple.h"
#include "executor/executor_context.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

void Updater::Init(concurrency::Transaction *txn, storage::DataTable *table,
                   Target *target_vector, uint32_t target_vector_size,
                   DirectMap *direct_map_vector,
                   uint32_t direct_map_vector_size) {
  PL_ASSERT(txn != nullptr && table != nullptr);
  PL_ASSERT(target_vector != nullptr && direct_map_vector != nullptr);

  txn_ = txn;
  table_ = table;

  target_list_.reset(new TargetList(target_vector,
                                    target_vector + target_vector_size));
  direct_map_list_.reset(new DirectMapList(direct_map_vector,
                                           direct_map_vector +
                                           direct_map_vector_size));
  target_vals_size_ = target_vector_size;
}

// Non primary key update : Two cases
//  1) If I am the owner, update on the original tuple
//  2) If I am NOT the owner, build a new version and update
void Updater::Update(uint32_t tile_group_id, uint32_t tuple_offset,
                     uint32_t *column_ids, char *target_vals,
                     executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  LOG_TRACE("Updating tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            tile_group_id, tuple_offset, table_->GetName().c_str(),
            table_->GetDatabaseOid(), table_->GetOid());

  peloton::type::Value *values =
     reinterpret_cast<peloton::type::Value *>(target_vals);
  uint32_t values_size = target_vals_size_;

  auto tile_group = table_->GetTileGroupById(tile_group_id).get();
  auto *tile_group_header = tile_group->GetHeader();
  ItemPointer old_location(tile_group_id, tuple_offset);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // Update In-Place
  auto is_owner = TransactionRuntime::IsOwner(*txn_, tile_group_header,
                                              tuple_offset);
  if (is_owner == true) {
    // Update on the original tuple
    ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_offset);

    // Set Target & Direct Map values
    for (uint32_t i = 0; i < values_size; i++)
      tuple.SetValue(column_ids[i], values[i]);
    for (auto dm : *direct_map_list_)
      tuple.SetValue(dm.first, tuple.GetValue(dm.second.second));

    txn_manager.PerformUpdate(txn_, old_location);
    executor_context->num_processed++;
    return;
  }

  // Build a new version tuple & update
  auto acquired = TransactionRuntime::AcquireOwnership(*txn_, tile_group_header,
                                                       tuple_offset);
  if (acquired == false)
    return;
  ItemPointer new_location = table_->AcquireVersion();
  auto new_tile_group =
      catalog::Manager::GetInstance().GetTileGroup(new_location.block);

  ContainerTuple<storage::TileGroup> old_tuple(tile_group, tuple_offset);
  ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
                                               new_location.offset);
  // Set Target & Direct Map values
  for (uint32_t i = 0; i < values_size; i++)
    new_tuple.SetValue(column_ids[i], values[i]);
  for (auto dm : *direct_map_list_)
    new_tuple.SetValue(dm.first, old_tuple.GetValue(dm.second.second));

  ItemPointer *indirection =
      tile_group_header->GetIndirection(old_location.offset);
  auto res = table_->InstallVersion(&new_tuple, target_list_.get(), txn_,
                                    indirection);
  if (res == false) {
    TransactionRuntime::YieldOwnership(*txn_, tile_group_header, tuple_offset);
    return;
  }
  txn_manager.PerformUpdate(txn_, old_location, new_location);
  executor_context->num_processed++;
}

// Primary key update : Delete the old tuple and insert a new tuple
void Updater::UpdatePrimaryKey(uint32_t tile_group_id, uint32_t tuple_offset,
                               uint32_t *column_ids, char *target_vals,
                               executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);
  LOG_TRACE("Updating tuple <%u, %u> from table '%s' (db ID: %u, table ID: %u)",
            tile_group_id, tuple_offset, table_->GetName().c_str(),
            table_->GetDatabaseOid(), table_->GetOid());
  peloton::type::Value *values =
     reinterpret_cast<peloton::type::Value *>(target_vals);
  uint32_t values_size = target_vals_size_;

  auto tile_group = table_->GetTileGroupById(tile_group_id).get();
  auto *tile_group_header = tile_group->GetHeader();
  auto is_owner = TransactionRuntime::IsOwner(*txn_, tile_group_header,
                                              tuple_offset);
  bool acquired_ownership = false;
  if (is_owner == false) {
    acquired_ownership = TransactionRuntime::AcquireOwnership(
        *txn_, tile_group_header, tuple_offset);
    if (acquired_ownership == false)
      return;
  }
  // 1. Delete the old tuple
  // TODO: InsertEmptyVersion and InsertTuple cannot exist without Delete in
  // the middle.  This looks a limit in the current index system.  In future,
  // we should be able to make all the storage ready and do the transaction work
  ItemPointer old_location(tile_group_id, tuple_offset);
  ItemPointer empty_location = table_->InsertEmptyVersion();
  if (empty_location.IsNull() == true && acquired_ownership == true) {
    TransactionRuntime::YieldOwnership(*txn_, tile_group_header,
                                       tuple_offset);
    return;
  }
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.PerformDelete(txn_, old_location, empty_location);

  // 2. Insert a new tuple
  ContainerTuple<storage::TileGroup> old_tuple(tile_group, tuple_offset);
  storage::Tuple new_tuple(table_->GetSchema(), true);

  // Set Target & Direct Map values
  for (uint32_t i = 0; i < values_size; i++)
    new_tuple.SetValue(column_ids[i], values[i]);
  for (auto dm : *direct_map_list_)
    new_tuple.SetValue(dm.first, old_tuple.GetValue(dm.second.second),
                        executor_context->GetPool());
  ItemPointer *index_entry_ptr = nullptr;
  ItemPointer new_location = table_->InsertTuple(&new_tuple, txn_,
                                                 &index_entry_ptr);
  if (new_location.block == INVALID_OID && acquired_ownership == true) {
    txn_manager.SetTransactionResult(txn_, ResultType::FAILURE);
    return;
  }
  txn_manager.PerformInsert(txn_, new_location, index_entry_ptr);
  executor_context->num_processed++;
}

void Updater::TearDown() {
  // Updater object does not destruct its own data structures
  target_list_.reset();
  direct_map_list_.reset();
}

}  // namespace codegen
}  // namespace peloton
