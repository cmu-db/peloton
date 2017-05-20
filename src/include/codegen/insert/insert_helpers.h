//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_helpers.h
//
// Identification: src/include/codegen/insert/insert_helpers.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/data_table.h"

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
  static bool InsertRawTuple(concurrency::Transaction *txn,
                             storage::DataTable *table,
                             const storage::Tuple *tuple);

  static void InsertValue(concurrency::Transaction *txn,
                          storage::DataTable *table, char *value,
                          size_t num_tuples);

  static storage::Tuple *CreateTuple(catalog::Schema *schema);

  static char *GetTupleData(storage::Tuple *tuple);

  static void DeleteTuple(storage::Tuple *tuple);
};

}  // namespace codegen
}  // namespace peloton
