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

namespace peloton {

namespace concurrency {
class TransactionContext;
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
  static uint32_t PerformVectorizedRead(concurrency::TransactionContext &txn,
                                        storage::TileGroup &tile_group,
                                        uint32_t tid_start, uint32_t tid_end,
                                        uint32_t *selection_vector);
  // Check Ownership
  static bool IsOwner(concurrency::TransactionContext &txn,
                      storage::TileGroupHeader *tile_group_header,
                      uint32_t tuple_offset);
  // Acquire Ownership
  static bool AcquireOwnership(concurrency::TransactionContext &txn,
                               storage::TileGroupHeader *tile_group_header,
                               uint32_t tuple_offset);
  // Yield Ownership
  // Note: this should be called when failed after acquired ownership
  //       otherwise, ownership is yielded inside transaction functions
  static void YieldOwnership(concurrency::TransactionContext &txn,
                             storage::TileGroupHeader *tile_group_header,
                             uint32_t tuple_offset);
};

}  // namespace codegen
}  // namespace peloton
