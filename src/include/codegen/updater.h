//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater.h
//
// Identification: src/include/codegen/updater.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "type/types.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class DataTable;
class TileGroup;
}  // namespace storage

namespace type {
class Value;
}  // namespace type

namespace codegen {
// This class handles updating tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Updater {
 public:
  // Initialize the instance
  void Init(concurrency::Transaction *txn, storage::DataTable *table,
            Target *target_vector, uint32_t target_vector_size,
            DirectMap *direct_map_vector,
            uint32_t direct_map_vector_size, bool update_primary_key);

  // Update a tuple
  void Update(uint32_t tile_group_id, uint32_t tuple_offset,
              uint32_t *col_ids, char *target_vals,
              executor::ExecutorContext *executor_context);

  void TearDown();

 private:
  // No external constructor
  Updater(): txn_(nullptr), table_(nullptr), target_vals_size_(0),
             update_primary_key_(false) {}

 private:
  // These are provided by the update translator
  concurrency::Transaction *txn_;
  storage::DataTable *table_;

  TargetList target_list_;
  DirectMapList direct_map_list_;
  uint32_t target_vals_size_;

  bool update_primary_key_;

 private:
  DISALLOW_COPY_AND_MOVE(Updater);
};

}  // namespace codegen
}  // namespace peloton
