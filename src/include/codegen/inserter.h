//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter.h
//
// Identification: src/include/codegen/inserter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "common/item_pointer.h"
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
class Tile;
class Tuple;
}  // namespace storage

namespace type {
class AbstractPool;
class EphemeralPool;
}  // namespace type

namespace codegen {
// This class handles insertion of tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Inserter {
 public:
  // Initializes the instance
  void Init(storage::DataTable *table,
            executor::ExecutorContext *executor_context);

  // Create the filter that checks the tuple can be inserted
  void CreateFilter();

  // Check whether the tuple can be inserted, i.e. not inserted by myself
  bool IsEligible(oid_t tile_group_id, oid_t tuple_offset);

  // Get the storage area that is to be reserved
  char *ReserveTupleStorage();

  // Get the pool address
  peloton::type::AbstractPool *GetPool();

  // Insert the tuple instance
  void InsertReserved();

  // Insert a tuple
  void Insert(const storage::Tuple *tuple);

  // Finalizes the instance
  void TearDown();

 private:
  // No external constructor
  Inserter(): table_(nullptr), executor_context_(nullptr), tile_(nullptr),
              filter_(nullptr) {}

 private:
  // Provided by its insert translator
  storage::DataTable *table_;
  executor::ExecutorContext *executor_context_;

  // Set once a tuple storage is reserved
  std::shared_ptr<storage::Tile> tile_;
  ItemPointer location_;

  // Tuples inserted from this INSERT execution
  std::unordered_set<ItemPointer, ItemPointerHasher, ItemPointerComparator>
      *filter_;

 private:
  DISALLOW_COPY_AND_MOVE(Inserter);
};

}  // namespace codegen
}  // namespace peloton
