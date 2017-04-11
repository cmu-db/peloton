//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_helpers.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/insert/insert_helpers.h"

#include "codegen/if.h"
#include "codegen/oa_hash_table_proxy.h"
#include "codegen/vectorized_loop.h"

namespace peloton {
namespace codegen {

bool InsertHelpers::InsertRawTuple(
    concurrency::TransactionManager *txn_mgr,
    concurrency::Transaction        *txn,
    storage::DataTable              *table,
    const storage::Tuple            *tuple) {

  // Insert tuple into the table.
  // Tuple will be copied.
  ItemPointer *index_entry_ptr = nullptr;
  ItemPointer location = table->InsertTuple(tuple, txn, &index_entry_ptr);

  // It is possible that some concurrent transactions have inserted the same
  // tuple. In this case, abort the transaction.
  if (location.block == INVALID_OID) {
    txn_mgr->SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }

  txn_mgr->PerformInsert(txn, location, index_entry_ptr);

  // @todo executor_context->num_processed += 1;

  return true;
}

}  // namespace codegen
}  // namespace peloton
