//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter.h
//
// Identification: src/include/codegen/deleter.cpp
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

class Deleter {
 public:
  void Init(concurrency::Transaction *txn, storage::DataTable *table);

  void Delete(uint32_t tile_group_id, uint32_t tuple_offset);

 private:
  // Can't construct
  Deleter() : txn_(nullptr), table_(nullptr) {}

 private:
  concurrency::Transaction *txn_;
  storage::DataTable *table_;

 private:
  DISALLOW_COPY_AND_MOVE(Deleter);
};

}  // namespace codegen
}  // namespace peloton