//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.h
//
// Identification: src/include/codegen/insert/insert_helpers.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/data_table.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator_translator.h"
#include "codegen/updateable_storage.h"

namespace peloton {
namespace codegen {

class InsertHelpers {
 public:

  /**
   * @brief Perform insertion once we already have a materialized raw tuple.
   */
  static bool InsertRawTuple(concurrency::TransactionManager *txn_mgr,
                             concurrency::Transaction *txn,
                             storage::DataTable *table,
                             const storage::Tuple *tuple);

  static void InsertValue(concurrency::Transaction *txn,
                          storage::DataTable *table,
                          type::Value *value);
};

}  // namespace codegen
}  // namespace peloton
