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
  void Update(storage::TileGroup *tile_group, uint32_t tuple_offset,
              uint32_t *col_ids, type::Value *target_vals,
              executor::ExecutorContext *executor_context);
 private:
  // No external constructor
  Updater(): txn_(nullptr), table_(nullptr), update_primary_key_(false),
             target_vector_(nullptr), target_vector_size_(0),
             direct_map_vector_(nullptr),direct_map_vector_size_(0) {}

 private:
  // These are provided by the update translator
  concurrency::Transaction *txn_;
  storage::DataTable *table_;
  bool update_primary_key_;
  Target *target_vector_;
  uint32_t target_vector_size_;
  DirectMap *direct_map_vector_;
  uint32_t direct_map_vector_size_;

 private:
  DISALLOW_COPY_AND_MOVE(Updater);
};

}  // namespace codegen
}  // namespace peloton
