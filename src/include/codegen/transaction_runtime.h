//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.h
//
// Identification: src/include/codegen/transaction_runtime.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "type/types.h"
#include "common/item_pointer.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace storage {
class DataTable;
class TileGroup;
class TileGroupHeader;
}  // namespace storage

namespace type {
class Value;
}  // namespace type

namespace codegen {

//===----------------------------------------------------------------------===//
// This class contains common runtime functions needed during query execution.
// These functions are used exclusively by the codegen component.
//===----------------------------------------------------------------------===//
class TransactionRuntime {
 public:
  // Perform a read operation for all tuples in the given tile group with IDs
  // in the range [tid_start, tid_end) in the context of the given transaction
  static uint32_t PerformVectorizedRead(concurrency::Transaction &txn,
                                        storage::TileGroup &tile_group,
                                        uint32_t tid_start, uint32_t tid_end,
                                        uint32_t *selection_vector);

  // Perform a delete operation: see more descriptions in the .cpp file
  static bool PerformDelete(concurrency::Transaction &txn,
                            storage::DataTable &table, uint32_t tile_group_id,
                            uint32_t tuple_offset);

  // Check Ownership
  static bool IsOwner(concurrency::Transaction &txn,
                      storage::TileGroupHeader *tile_group_header,
                      uint32_t tuple_offset);
  // Acquire Ownership
  static bool AcquireOwnership(concurrency::Transaction &txn,
                               storage::TileGroupHeader *tile_group_header,
                               uint32_t tuple_offset);
  // Yield Ownership
  // Note: this should be called when failed after acquired ownership
  //       otherwise, ownership is yielded inside transaction functions
  static void YieldOwnership(concurrency::Transaction &txn,
                             storage::TileGroupHeader *tile_group_header,
                             uint32_t tuple_offset);

  // Perform an in-place update operation
  // Note: One should be an owner or have acquired the ownership
  static void PerformUpdate(concurrency::Transaction &txn,
      ItemPointer &location, executor::ExecutorContext *executor_context);

  // Perform a new version update operation
  // Note: One should be an owner or have acquired the ownership
  static void PerformUpdate(concurrency::Transaction &txn,
      ItemPointer &old_location, ItemPointer &new_location,
      executor::ExecutorContext *executor_context);

  // Perform a primary key update operation by PerformDelete + PerformInsert
  // Note: One should be an owner or have acquired the ownership
  static void PerformDelete(concurrency::Transaction &txn,
      ItemPointer &old_location, ItemPointer &empty_location);
  static void PerformInsert(concurrency::Transaction &txn,
      ItemPointer &new_location, ItemPointer *index_entry_ptr,
      executor::ExecutorContext *executor_context);

  static void IncreaseNumProcessed(executor::ExecutorContext *executor_context);

  static void SetTransactionFailure(concurrency::Transaction &txn);
};

}  // namespace codegen
}  // namespace peloton
