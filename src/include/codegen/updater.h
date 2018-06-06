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

#include "common/item_pointer.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace storage {
class DataTable;
class Tile;
}  // namespace storage

namespace type {
class AbstractPool;
}  // namespace concurrency

namespace codegen {
// This class handles updating tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Updater {
 public:
  // Initialize the instance
  void Init(storage::DataTable *table,
            executor::ExecutorContext *executor_context,
            Target *target_vector, uint32_t target_vector_size);

  // Prepare for a non-primary key update and get a tuple data pointer
  char *Prepare(uint32_t tile_group_id, uint32_t tuple_offset);

  // Prepare for a primary key update and get a tuple data pointer
  char *PreparePK(uint32_t tile_group_id, uint32_t tuple_offset);

  // Get Pool after Prepare() or PreparePK()
  peloton::type::AbstractPool *GetPool();

  // Update a tuple
  void Update();

  // Update a tuple with primary key
  void UpdatePK();

  // Finalize the instance
  void TearDown();

 private:
  // No external constructor
  Updater(): table_(nullptr), executor_context_(nullptr), target_list_(nullptr),
             is_owner_(false), acquired_ownership_(false), tile_(nullptr) {}

  char *GetDataPtr(uint32_t tile_group_id, uint32_t tuple_offset);

  // Check if the tuple is in the statement write set
  inline bool IsInStatementWriteSet(ItemPointer location) {
    return statement_write_set_->find(location) !=
           statement_write_set_->end();
  }

  // Add the updated location to the statement write set
  inline void AddToStatementWriteSet(ItemPointer& location) {
    statement_write_set_->insert(location);
  }

 private:
  // Table and executor context from the update translator
  storage::DataTable *table_;
  executor::ExecutorContext *executor_context_;

  // Target list and direct map list pointer from the update translator
  TargetList *target_list_;

  // Write set for tracking newly created tuples inserted by the same statement
  // This statement-level write set is essential for avoiding the Halloween Problem,
  // which refers to the phenomenon that an update operation causes a change to
  // a tuple, potentially allowing this tuple to be visited more than once during
  // the same operation.
  // By maintaining the statement-level write set, an update operation will check 
  // whether the to-be-updated tuple is created by the same operation.
  WriteSet *statement_write_set_;

  // Ownership information
  bool is_owner_;
  bool acquired_ownership_;

  // Itempointers holding the previous location and the new location
  ItemPointer old_location_;
  ItemPointer new_location_;

  // Tile info used for retrieving the tuple location
  std::shared_ptr<storage::Tile> tile_;

 private:
  DISALLOW_COPY_AND_MOVE(Updater);
};

}  // namespace codegen
}  // namespace peloton
