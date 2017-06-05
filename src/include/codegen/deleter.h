//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter.h
//
// Identification: src/include/codegen/deleter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "common/macros.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

// This class handles deletion of tuples from generated code. It mainly exists
// to avoid passing along table information through translators. Instead, this
// class is initialized once (through Init()) outside the main loop.
class Deleter {
 public:

  // Initializer this deleter instance using the provided transaction and table.
  // All tuples to be deleted occur within the provided transaction are from
  // the provided table
  void Init(concurrency::Transaction *txn, storage::DataTable *table);

  // Delete the tuple within the provided tile group ID (unique) at the provided
  // offset from the start of the tile group.
  void Delete(uint32_t tile_group_id, uint32_t tuple_offset);

 private:
  // Can't construct
  Deleter() : txn_(nullptr), table_(nullptr) {}

 private:
  // The transaction deletions happen in
  concurrency::Transaction *txn_;

  // The table the tuples are deleted from
  storage::DataTable *table_;

 private:
  DISALLOW_COPY_AND_MOVE(Deleter);
};

}  // namespace codegen
}  // namespace peloton