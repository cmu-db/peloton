//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.cpp
//
// Identification: src/codegen/transaction_runtime.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_runtime.h"

#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

// Perform a read operation for all tuples in the tile group in the given range
// TODO: Right now, we split this check into two loops: a visibility check and
//       the actual reading. Can this be merged?
uint32_t TransactionRuntime::PerformVectorizedRead(
    concurrency::Transaction &txn, storage::TileGroup &tile_group,
    uint32_t tid_start, uint32_t tid_end, uint32_t *selection_vector) {
  // Get the transaction manager
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Get the tile group header
  auto tile_group_header = tile_group.GetHeader();

  // Check visibility of tuples in the range [tid_start, tid_end), storing all
  // visible tuple IDs in the provided selection vector
  uint32_t out_idx = 0;
  for (uint32_t i = tid_start; i < tid_end; i++) {
    // Perform the visibility check
    auto visibility = txn_manager.IsVisible(&txn, tile_group_header, i);

    // Update the output position
    selection_vector[out_idx] = i;
    out_idx += (visibility == VisibilityType::OK);
  }

  uint32_t tile_group_idx = tile_group.GetTileGroupId();

  // Perform a read operation for every visible tuple we found
  uint32_t end_idx = out_idx;
  out_idx = 0;
  for (uint32_t idx = 0; idx < end_idx; idx++) {
    // Construct the item location
    ItemPointer location{tile_group_idx, selection_vector[idx]};

    // Perform the read
    bool can_read = txn_manager.PerformRead(&txn, location);

    // Update the selection vector and output position
    selection_vector[out_idx] = selection_vector[idx];
    out_idx += static_cast<uint32_t>(can_read);
  }

  return out_idx;
}

bool TransactionRuntime::PerformDelete(concurrency::Transaction &txn,
                                       storage::DataTable &table,
                                       uint32_t tile_group_id,
                                       uint32_t tuple_offset) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto tile_group = table.GetTileGroupById(tile_group_id);
  ItemPointer old_location(tile_group_id, tuple_offset);

  auto *tile_group_header = tile_group->GetHeader();

  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_written =
      txn_manager.IsWritten(&txn, tile_group_header, tuple_offset);

  if (is_owner && is_written) {
    LOG_TRACE("I am the owner of the tuple");
    txn_manager.PerformDelete(&txn, old_location);
    return true;
  }

  bool is_ownable =
      is_owner || txn_manager.IsOwnable(&txn, tile_group_header, tuple_offset);
  if (!is_ownable) {
    // transaction should be aborted as we cannot update the latest version.
    LOG_TRACE("Fail to delete tuple.");
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }
  LOG_TRACE("I am NOT the owner, but it is visible");

  bool acquired_ownership =
      is_owner ||
      txn_manager.AcquireOwnership(&txn, tile_group_header, tuple_offset);
  if (!acquired_ownership) {
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }

  LOG_TRACE("Ownership is acquired");
  // if it is the latest version and not locked by other threads, then
  // insert an empty version.
  ItemPointer new_location = table.InsertEmptyVersion();

  // PerformDelete() will not be executed if the insertion failed.
  if (new_location.IsNull()) {
    LOG_TRACE("Fail to insert a new tuple version and so fail to delete");
    // this means we acquired ownership from AcquireOwnership
    if (!is_owner) {
      // A write lock is acquired when inserted, but it is not in the write set,
      // because we haven't yet put it into the write set.
      // The acquired lock is not released when the txn gets aborted, and
      // YieldOwnership() will help us release the acquired write lock.
      txn_manager.YieldOwnership(&txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformDelete(&txn, old_location, new_location);
  return true;
}

void SetTargetValues(AbstractTuple *dest, uint32_t values_size,
                     uint32_t *column_ids, peloton::type::Value *values) {
  for (uint32_t i = 0; i < values_size; i++) {
    dest->SetValue(column_ids[i], values[i]);
  }
}

void SetDirectMapValues(AbstractTuple *dest, const AbstractTuple *src,
                        DirectMapList direct_list) {
  for (auto dm : direct_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    auto value = src->GetValue(src_col_id);
    dest->SetValue(dest_col_id, value);
  }
}

void SetDirectMapValues(storage::Tuple *dest, const AbstractTuple *src,
                        DirectMapList direct_list,
                        peloton::type::AbstractPool *pool) {
  for (auto dm : direct_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    auto value = (src->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value, pool);
  }
}

bool PerformUpdatePrimaryKey(concurrency::Transaction *txn,
                             storage::DataTable &table,
                             oid_t tile_group_id, oid_t tuple_offset,
                             storage::TileGroup *tile_group,
                             uint32_t *column_ids, peloton::type::Value *values,
                             uint32_t values_size,
                             DirectMapList direct_map_list, bool is_owner,
                             executor::ExecutorContext *executor_context) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // We delete tuple/version chain
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  ItemPointer old_location(tile_group_id, tuple_offset);
  ItemPointer new_location = table.InsertEmptyVersion();
  // There is a write lock acquired, but since it is not in the write set,
  // because we haven't yet put them into the write set.
  // The acquired lock can't be released by Yield...() when the txn is aborted.
  if (new_location.IsNull() == true) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    if (is_owner == false) {
      txn_manager.YieldOwnership(txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }

  txn_manager.PerformDelete(txn, old_location, new_location);

  // Insert tuple rather than install version
  storage::Tuple new_tuple(table.GetSchema(), true);
  ContainerTuple<storage::TileGroup> old_tuple(tile_group, tuple_offset);
  SetTargetValues(&new_tuple, values_size, column_ids, values);
  SetDirectMapValues(&new_tuple, &old_tuple, direct_map_list,
                     executor_context->GetPool());

  ItemPointer *index_entry_ptr = nullptr;
  peloton::ItemPointer location = table.InsertTuple(&new_tuple, txn,
                                                    &index_entry_ptr);
  // Another concurrent transaction may have inserted the same tuple, just abort
  if (location.block == INVALID_OID) {
    txn_manager.SetTransactionResult(txn, peloton::ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformInsert(txn, location, index_entry_ptr);
  return true;
}

bool TransactionRuntime::PerformUpdate(concurrency::Transaction &txn,
    storage::DataTable &table, uint32_t tile_group_id, uint32_t tuple_offset,
    uint32_t *column_ids, peloton::type::Value *values, uint32_t values_size,
    TargetList &target_list, DirectMapList &direct_map_list,
    bool update_primary_key, executor::ExecutorContext *executor_context) {
  auto tile_group = table.GetTileGroupById(tile_group_id).get();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  ItemPointer old_location(tile_group_id, tuple_offset);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_written = txn_manager.IsWritten(&txn, tile_group_header,
                                          tuple_offset);
  PL_ASSERT((is_owner == false && is_written == true) == false);
  // We have already owned a version
  if (is_owner == true && is_written == true) {
    if (update_primary_key == true) {
      return PerformUpdatePrimaryKey(&txn, table, tile_group_id,
          tuple_offset, tile_group, column_ids, values,
          values_size, direct_map_list, is_owner, executor_context);
    }
    // Make a copy of the original tuple and overwrite
    ContainerTuple<storage::TileGroup> old_tuple(tile_group, tuple_offset);
    SetTargetValues(&old_tuple, values_size, column_ids, values);
    SetDirectMapValues(&old_tuple, &old_tuple, direct_map_list);
    txn_manager.PerformUpdate(&txn, old_location);
    return true;
  }

  // We have not owned a version, but let's see if it is ownable from now on
  bool is_ownable = is_owner || txn_manager.IsOwnable(&txn, tile_group_header,
                                                      tuple_offset);
  if (is_ownable == false) {
    // transaction should be aborted as we cannot update the latest version.
    LOG_TRACE("Fail to update tuple. Set txn failure.");
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }

  // If the tuple is not owned by any other txn and is ownable to me
  bool acquire_ownership_success = is_owner ||
      txn_manager.AcquireOwnership(&txn, tile_group_header, tuple_offset);
  if (acquire_ownership_success == false) {
    LOG_TRACE("Fail to update tuple. Set txn failure.");
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }

  // We have acquired the ownership, and so ...
  if (update_primary_key == true) {
    return PerformUpdatePrimaryKey(&txn, table, tile_group_id,
        tuple_offset, tile_group, column_ids, values,
        values_size, direct_map_list, is_owner, executor_context);
  }

  // If it is the latest version and not locked by other threads, then
  // insert a new version.  Acquire a version slot from the table.
  ContainerTuple<storage::TileGroup> old_tuple(tile_group, tuple_offset);
  ItemPointer new_location = table.AcquireVersion();
  auto &manager = catalog::Manager::GetInstance();
  auto new_tile_group = manager.GetTileGroup(new_location.block);
  ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
                                               new_location.offset);
  // This triggers in-place update and we don't need to allocate another version
  SetTargetValues(&new_tuple, values_size, column_ids, values);
  SetDirectMapValues(&new_tuple, &old_tuple, direct_map_list);
  ItemPointer *indirection =
      tile_group_header->GetIndirection(old_location.offset);
  auto ret = table.InstallVersion(&new_tuple, &target_list, &txn, indirection);
  // There is a write lock acquired, but since it is not in the write set,
  // because we haven't yet put them into the write set.
  // The acquired lock can't be released by Yield...() when the txn is aborted.
  if (ret == false) {
    LOG_TRACE("Fail to insert new tuple while updating. Set txn failure.");
    if (is_owner == false) {
      txn_manager.YieldOwnership(&txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }

  LOG_TRACE("Perform update old location: %u, %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Perform update new location: %u, %u", new_location.block,
            new_location.offset);

  txn_manager.PerformUpdate(&txn, old_location, new_location);
  return true;
}

void TransactionRuntime::IncreaseNumProcessed(
    executor::ExecutorContext *executor_context) {
  executor_context->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
