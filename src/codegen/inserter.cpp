//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter.cpp
//
// Identification: src/codegen/inserter.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/inserter.h"
#include "codegen/transaction_runtime.h"
#include "executor/executor_context.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

void Inserter::Init(concurrency::Transaction *txn, storage::DataTable *table,
                    executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn != nullptr && table != nullptr);
  txn_ = txn;
  table_ = table;
  executor_context_ = executor_context;
}

void Inserter::CreateTuple() {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);
  tuple_.reset(new storage::Tuple(table_->GetSchema(), true));
  pool_.reset(new peloton::type::EphemeralPool());
}

char *Inserter::GetTupleData() { return tuple_->GetData(); }

peloton::type::AbstractPool *Inserter::GetPool() { return pool_.get(); }

void Inserter::InsertTuple() { Insert(tuple_.get()); }

void Inserter::Insert(const storage::Tuple *tuple) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  auto result = TransactionRuntime::PerformInsert(*txn_, *table_, tuple);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context_);
  }
}

void Inserter::Destroy() {
  tuple_.reset(nullptr);
  pool_.reset(nullptr);
}

}  // namespace codegen
}  // namespace peloton
