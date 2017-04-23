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
#include "storage/tuple.h"

namespace peloton {
namespace codegen {

bool InsertHelpers::InsertRawTuple(concurrency::Transaction *txn,
                                   storage::DataTable *table,
                                   const storage::Tuple *tuple) {

  concurrency::TransactionManager *txn_mgr =
      &concurrency::TransactionManagerFactory::GetInstance();

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
                                char *value, size_t num_tuples) {

  const storage::Tuple * const *tuples =
      reinterpret_cast<const storage::Tuple * const *>(value);

  num_tuples /= sizeof(const storage::Tuple *);

  LOG_DEBUG("num_tuples = %zu", num_tuples);

  for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {
    const storage::Tuple *tuple = tuples[i];
    LOG_DEBUG("tuple[%zu] = %p", i, tuple);

    InsertRawTuple(txn, table, tuple);
  }
}

storage::Tuple *InsertHelpers::CreateTuple(catalog::Schema *schema) {
  storage::Tuple *tuple = new storage::Tuple(schema, true);
  PL_ASSERT(tuple != nullptr);

  LOG_DEBUG("Created tuple: %p", tuple);

  return tuple;
}

char *InsertHelpers::GetTupleData(storage::Tuple *tuple) {
  char *data = tuple->GetData();

  LOG_DEBUG("GetTupleData(%p) = %p", tuple, data);
  return data;
}

void InsertHelpers::DeleteTuple(storage::Tuple *tuple) {
  delete tuple;
}

}  // namespace codegen
}  // namespace peloton
