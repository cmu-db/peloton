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
  TransactionPool(const size_t &max_concurrency) 
    : max_concurrency_(max_concurrency),
      transactions_(max_concurrency) { 

    for (size_t i = 0; i < max_concurrency; ++i) {
      transactions_.Enqueue(new Transaction());
    }
  }

  ~TransactionPool() {
    for (size_t i = 0; i < max_concurrency_; ++i) {
      Transaction *txn;
      transactions_.Dequeue(txn);
      delete txn;
      txn = nullptr;
    }
  }

  void AcquireTransaction(Transaction *txn) {
    while (transactions_.Dequeue(txn) == false);
  }

  bool TryAcquireTransaction(Transaction *txn) {
      return transactions_.Dequeue(txn);
  }

  void ReleaseTransaction(Transaction *txn) {
    transactions_.Enqueue(txn);
  }

  size_t GetMaxConcurrency() {
    return max_concurrency_;
  }

 private:
  size_t max_concurrency_;
  peloton::LockFreeQueue<Transaction*> transactions_;

};

}  // End concurrency namespace
}  // End peloton namespace
