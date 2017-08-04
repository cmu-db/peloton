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

void DoProjection(AbstractTuple *dest, const AbstractTuple *tuple,
                  peloton::type::Value *values, uint32_t *col_ids,
                  uint32_t target_size, DirectMapList direct_list) {
  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    peloton::type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value);
  }
}

void DoProjection(storage::Tuple *dest, const AbstractTuple *tuple,
                  peloton::type::Value *values, uint32_t *col_ids,
                  uint32_t target_size, DirectMapList direct_map_list,
                  executor::ExecutorContext *context = nullptr) {
  peloton::type::AbstractPool *pool = nullptr;
  if (context != nullptr) pool = context->GetPool();

  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_map_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    peloton::type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value, pool);
  }
}

bool PerformUpdatePrimaryKey(concurrency::Transaction *txn, bool is_owner,
                             storage::TileGroupHeader *tile_group_header,
                             storage::DataTable &table,
                             oid_t tuple_offset, ItemPointer &old_location,
                             storage::TileGroup *tile_group,
                             peloton::type::Value *values, uint32_t *col_ids,
                             uint32_t target_size,
                             DirectMapList direct_map_list,
                             executor::ExecutorContext *executor_context) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // We delete tuple/version chain
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
  expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                           tuple_offset);
  DoProjection(&new_tuple, &old_tuple, values, col_ids, target_size,
               direct_map_list, executor_context);

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
    storage::DataTable &table, uint32_t tile_group_id,
    uint32_t tuple_offset, uint32_t *col_ids, peloton::type::Value *target_vals,
    bool update_primary_key, Target *target_vector, uint32_t target_vector_size,
    DirectMap *direct_map_vector, uint32_t direct_map_vector_size,
    executor::ExecutorContext *executor_context) {

  auto tile_group = table.GetTileGroupById(tile_group_id).get();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  // Deserialize the vectors
  TargetList target_list;
  for (uint32_t i = 0; i < target_vector_size; i++) {
    target_list.emplace_back(target_vector[i]);
  }
  DirectMapList direct_map_list;
  for (uint32_t i = 0; i < direct_map_vector_size; i++) {
    direct_map_list.emplace_back(direct_map_vector[i]);
  }

  ItemPointer old_location(tile_group_id, tuple_offset);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_written = txn_manager.IsWritten(&txn, tile_group_header,
                                          tuple_offset);
  PL_ASSERT((is_owner == false && is_written == true) == false);

  // We have already owned a version
  bool ret = true;
  if (is_owner == true && is_written == true) {
    if (update_primary_key == true) {
      ret = PerformUpdatePrimaryKey(&txn, is_owner, tile_group_header, table,
          tuple_offset, old_location, tile_group, target_vals, col_ids,
          target_vector_size, direct_map_list, executor_context);
      return ret;
    }
    // Make a copy of the original tuple and allocate a new tuple
    expression::ContainerTuple<storage::TileGroup> old_tuple(
        tile_group, tuple_offset);
    DoProjection(&old_tuple, &old_tuple, target_vals, col_ids,
                 target_vector_size, direct_map_list);
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
    ret = PerformUpdatePrimaryKey(&txn, is_owner, tile_group_header, table,
        tuple_offset, old_location, tile_group, target_vals, col_ids,
        target_vector_size, direct_map_list, executor_context);
    return ret;
  }

  // If it is the latest version and not locked by other threads, then
  // insert a new version.  Acquire a version slot from the table.
  expression::ContainerTuple<storage::TileGroup> old_tuple(
      tile_group, tuple_offset);
  ItemPointer new_location = table.AcquireVersion();
  auto &manager = catalog::Manager::GetInstance();
  auto new_tile_group = manager.GetTileGroup(new_location.block);
  expression::ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
                                                           new_location.offset);
  // This triggers in-place update and we don't need to allocate another version
  DoProjection(&new_tuple, &old_tuple, target_vals, col_ids, target_vector_size,
               direct_map_list);

  ItemPointer *indirection =
      tile_group_header->GetIndirection(old_location.offset);
  ret = table.InstallVersion(&new_tuple, &target_list, &txn, indirection);
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
