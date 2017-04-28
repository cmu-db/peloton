//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.h
//
// Identification: src/include/codegen/transaction_runtime.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class TileGroup;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// This class contains common runtime functions needed during query execution.
// These functions are used exclusively by the codegen component.
//===----------------------------------------------------------------------===//
class TransactionRuntime {

  // Perform a read operation for all tuples in the given tile group with IDs
  // in the range [tid_start, tid_end) in the context of the given transaction
  static uint32_t PerformVectorizedRead(concurrency::Transaction &txn,
                                        storage::TileGroup &tile_group,
                                        uint32_t tid_start, uint32_t tid_end,
                                        uint32_t *selection_vector);

  // Add other stuff for Insert/Update/Delete

};

}  // namespace codegen
}  // namespace peloton