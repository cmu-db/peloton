//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_pool.h
//
// Identification: src/include/concurrency/transaction_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "container/lock_free_queue.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// TransactionPool
//===--------------------------------------------------------------------===//

class TransactionPool {
  TransactionPool(TransactionPool const &) = delete;

 public:
  TransactionPool() { 

    transactions_ = new peloton::LockFreeQueue<Transaction*>*[scalability_hint_];

    size_t k = max_concurrency_ / scalability_hint_;
    // initiate all the transactions  
    for (size_t i = 0; i < scalability_hint_; ++i) {
      transactions_[i] = new peloton::LockFreeQueue<Transaction*>(k);

      for (size_t j = 0; j < k; ++j) {
        Transaction *txn = new Transaction(i * k + j);
        txn->SetPoolHint(i);

        transactions_[i]->Enqueue(txn);
      }
    }
  }

  ~TransactionPool() {
    
    size_t k = max_concurrency_ / scalability_hint_;
    // deconstruct all the transactions
    for (size_t i = 0; i < scalability_hint_; ++i) {
      for (size_t j = 0; j < k; ++j) {
        Transaction *txn = nullptr;
        transactions_[i]->Dequeue(txn);
        delete txn;
        txn = nullptr;
      }
      delete transactions_[i];
      transactions_[i] = nullptr;
    }
    delete[] transactions_;
    transactions_ = nullptr;
  }

  static TransactionPool &GetInstance() {
    static TransactionPool transaction_pool;
    return transaction_pool;
  }

  static void Configure(const size_t max_concurrency, const size_t scalability_hint) {
    max_concurrency_ = max_concurrency;
    scalability_hint_ = scalability_hint;
  }

  void AcquireTransaction(Transaction *&txn, const size_t hint) {
    while (transactions_[hint]->Dequeue(txn) == false);
  }

  bool TryAcquireTransaction(Transaction *&txn, const size_t hint) {
    return transactions_[hint]->Dequeue(txn);
  }

  void ReleaseTransaction(Transaction *txn) {
    size_t hint = txn->GetPoolHint();
    transactions_[hint]->Enqueue(txn);
  }

  size_t GetMaxConcurrency() {
    return max_concurrency_;
  }

  size_t GetScalabilityHint() {
    return scalability_hint_;
  }

 private:
  static size_t max_concurrency_;
  static size_t scalability_hint_;
  peloton::LockFreeQueue<Transaction*> **transactions_;

};


}  // End concurrency namespace
}  // End peloton namespace
