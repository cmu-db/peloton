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

  // Perform an update operation
  static bool PerformUpdate(concurrency::Transaction &txn,
                            storage::DataTable &table, uint32_t tile_group_id,
                            uint32_t tuple_offset, uint32_t *column_ids,
                            peloton::type::Value *values, uint32_t values_size,
                            TargetList &target_list,
                            DirectMapList &direct_map_list,
                            bool update_primary_key,
                            executor::ExecutorContext *executor_context);

  static void IncreaseNumProcessed(executor::ExecutorContext *executor_context);
};

}  // namespace codegen
}  // namespace peloton
