//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater.cpp
//
// Identification: src/codegen/updater.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updater.h"
#include "codegen/transaction_runtime.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

void Updater::Init(concurrency::Transaction *txn, storage::DataTable *table,
                   Target *target_vector, uint32_t target_vector_size,
                   DirectMap *direct_map_vector,
                   uint32_t direct_map_vector_size, bool update_primary_key) {
  PL_ASSERT(txn != nullptr && table != nullptr);
  PL_ASSERT(target_vector != nullptr && direct_map_vector != nullptr);

  txn_ = txn;
  table_ = table;
  update_primary_key_ = update_primary_key;

  target_vector_ = target_vector;
  target_vector_size_ = target_vector_size;
  direct_map_vector_ = direct_map_vector;
  direct_map_vector_size_ = direct_map_vector_size;
}

void Updater::Update(uint32_t tile_group_id, uint32_t tuple_offset,
                     uint32_t *col_ids, char *target_vals,
                     executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  peloton::type::Value *values =
     reinterpret_cast<peloton::type::Value *>(target_vals);
  auto result = TransactionRuntime::PerformUpdate(*txn_, *table_, tile_group_id,
                                                  tuple_offset, col_ids, 
                                                  values, update_primary_key_,
                                                  target_vector_,
                                                  target_vector_size_,
                                                  direct_map_vector_,
                                                  direct_map_vector_size_,
                                                  executor_context);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context);
  }
}

}  // namespace codegen
}  // namespace peloton
