//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/pessimistic_transaction_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "pessimistic_transaction_manager.h"

namespace peloton {
namespace concurrency {
PessimisticTransactionManager &PessimisticTransactionManager::GetInstance() {
  static PessimisticTransactionManager txn_manager;
  return txn_manager;
}

// Visibility check
bool PessimisticTransactionManager::IsVisible(const txn_id_t &tuple_txn_id __attribute__((unused)),
                                              const cid_t &tuple_begin_cid __attribute__((unused)),
                                              const cid_t &tuple_end_cid __attribute__((unused))) {
  return true;
}

void PessimisticTransactionManager::CommitTransaction() { delete current_txn; }

void PessimisticTransactionManager::AbortTransaction() { delete current_txn; }
}
}