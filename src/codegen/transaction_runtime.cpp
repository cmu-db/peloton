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

bool TransactionRuntime::IsOwner(concurrency::Transaction &txn,
                                 storage::TileGroupHeader *tile_group_header,
                                 uint32_t tuple_offset) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_written = txn_manager.IsWritten(&txn, tile_group_header,
                                          tuple_offset);
  PL_ASSERT((is_owner == false && is_written == true) == false);
  if (is_owner == true && is_written == true)
    return true;
  return false;
}

bool TransactionRuntime::AcquireOwnership(concurrency::Transaction &txn,
    storage::TileGroupHeader *tile_group_header, uint32_t tuple_offset) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_ownable = is_owner ||
                    txn_manager.IsOwnable(&txn, tile_group_header,
                                          tuple_offset);
  if (is_ownable == false) {
    // transaction should be aborted as we cannot update the latest version.
    LOG_TRACE("Not ownable. Fail to update tuple. Txn failure.");
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }

  // If the tuple is not owned by any other txn and is ownable to me
  bool acquired = is_owner ||
                  txn_manager.AcquireOwnership(&txn, tile_group_header,
                                               tuple_offset);
  if (acquired == false) {
    LOG_TRACE("Cannot acquire ownership. Fail to update tuple. Txn failure.");
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }
  return true;
}

void TransactionRuntime::YieldOwnership(concurrency::Transaction &txn,
    storage::TileGroupHeader *tile_group_header, uint32_t tuple_offset) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  if (is_owner == false) {
      txn_manager.YieldOwnership(&txn, tile_group_header, tuple_offset);
  }
  txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
}

void TransactionRuntime::PerformUpdate(concurrency::Transaction &txn,
    ItemPointer &location, executor::ExecutorContext *executor_context) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.PerformUpdate(&txn, location);
  IncreaseNumProcessed(executor_context);
}

void TransactionRuntime::PerformUpdate(concurrency::Transaction &txn,
    ItemPointer &old_location, ItemPointer &new_location,
    executor::ExecutorContext *executor_context) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  LOG_TRACE("Perform update: old(%u,%u) new(%u, %u)", old_location.block,
            old_location.offset, new_location.block, new_location.offset);

  txn_manager.PerformUpdate(&txn, old_location, new_location);
  IncreaseNumProcessed(executor_context);
}

void TransactionRuntime::PerformDelete(concurrency::Transaction &txn,
    ItemPointer &old_location, ItemPointer &empty_location) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  txn_manager.PerformDelete(&txn, old_location, empty_location);
}

void TransactionRuntime::PerformInsert(concurrency::Transaction &txn,
    ItemPointer &new_location, ItemPointer *index_entry_ptr,
    executor::ExecutorContext *executor_context) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  txn_manager.PerformInsert(&txn, new_location, index_entry_ptr);
  IncreaseNumProcessed(executor_context);
}

void TransactionRuntime::SetTransactionFailure(concurrency::Transaction &txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
}

void TransactionRuntime::IncreaseNumProcessed(
    executor::ExecutorContext *executor_context) {
  executor_context->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
