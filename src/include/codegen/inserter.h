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

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class DataTable;
class Tuple;
}  // namespace storage

namespace type {
class EphemeralPool;
}  // namespace type

namespace codegen {
// This class handles insertion of tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Inserter {
 public:
  // Initializes the instance
  void Init(concurrency::Transaction *txn, storage::DataTable *table);

  // Create the tuple instance
  void CreateTuple();

  // Get the tuple's data pointer
  char *GetTupleData();

  // Get the pool address
  type::AbstractPool *GetPool();

  // Insert the tuple instance
  void InsertTuple();

  // Insert a tuple
  void Insert(const storage::Tuple *tuple);

  // Destroy all the resources
  void Destroy();

 private:
  // No external constructor
  Inserter(): txn_(nullptr), table_(nullptr), tuple_(nullptr), pool_(nullptr) {}

 private:
  // These are provided by its insert translator
  concurrency::Transaction *txn_;
  storage::DataTable *table_;

  // These are created and locally managed
  std::unique_ptr<storage::Tuple> tuple_;
  std::unique_ptr<type::EphemeralPool> pool_;

 private:
  DISALLOW_COPY_AND_MOVE(Inserter);
};

}  // namespace codegen
}  // namespace peloton
