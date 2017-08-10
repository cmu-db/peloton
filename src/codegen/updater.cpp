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
#include "planner/project_info.h"
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

  for (uint32_t i = 0; i < target_vector_size; i++) {
    target_list_.emplace_back(target_vector[i]);
  }
  for (uint32_t i = 0; i < direct_map_vector_size; i++) {
    direct_map_list_.emplace_back(direct_map_vector[i]);
  }
  target_vals_size_ = target_vector_size;

  update_primary_key_ = update_primary_key;
}

void Updater::Update(uint32_t tile_group_id, uint32_t tuple_offset,
                     uint32_t *column_ids, char *target_vals,
                     executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  peloton::type::Value *values =
     reinterpret_cast<peloton::type::Value *>(target_vals);
 auto result = TransactionRuntime::PerformUpdate(*txn_, *table_, tile_group_id,
                                                  tuple_offset, column_ids,
                                                  values, target_vals_size_,
                                                  target_list_,
                                                  direct_map_list_,
                                                  update_primary_key_,
                                                  executor_context);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context);
  }
}

void Updater::TearDown() {
  // Updater object does not destruct its own data structures
  target_list_.~TargetList();
  direct_map_list_.~DirectMapList();
}

}  // namespace codegen
}  // namespace peloton
