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

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "common/item_pointer.h"

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

  // Allocate the storage area that is to be reserved
  char *AllocateTupleStorage();

  // Get the pool address
  peloton::type::AbstractPool *GetPool();

  // Insert a tuple
  void Insert();

  // Finalize the instance
  void TearDown();

 private:
  // No external constructor
  Inserter(): table_(nullptr), executor_context_(nullptr), tile_(nullptr) {}

 private:
  // Provided by its insert translator
  storage::DataTable *table_;
  executor::ExecutorContext *executor_context_;

  // Set once a tuple storage is reserved
  std::shared_ptr<storage::Tile> tile_;
  ItemPointer location_;

 private:
  DISALLOW_COPY_AND_MOVE(Inserter);
};

}  // namespace codegen
}  // namespace peloton
