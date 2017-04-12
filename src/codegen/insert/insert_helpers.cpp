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

#include "concurrency/transaction_manager_factory.h"
#include "codegen/insert/insert_helpers.h"

namespace peloton {
namespace codegen {

bool InsertHelpers::InsertRawTuple(
    concurrency::TransactionManager *txn_mgr,
    concurrency::Transaction *txn,
    storage::DataTable *table,
    const storage::Tuple *tuple) {

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

void InsertHelpers::InsertValue(concurrency::Transaction *txn,
                                storage::DataTable *table,
                                type::Value *value) {

  const storage::Tuple * const *tuples =
      reinterpret_cast<const storage::Tuple * const *>(value->GetData());

  uint32_t num_tuples = value->GetLength() / sizeof(const storage::Tuple *);

  LOG_DEBUG("num_tuples = %u", num_tuples);

  concurrency::TransactionManager *txn_mgr =
      &concurrency::TransactionManagerFactory::GetInstance();

  for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {
    const storage::Tuple *tuple = tuples[i];
    LOG_DEBUG("tuple[%u] = %p", i, tuple);

    InsertRawTuple(txn_mgr, txn, table, tuple);
  }
}

}  // namespace codegen
}  // namespace peloton
