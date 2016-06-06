//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager_factory.h
//
// Identification: src/include/concurrency/transaction_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/optimistic_txn_manager.h"
#include "concurrency/pessimistic_txn_manager.h"
#include "concurrency/speculative_read_txn_manager.h"
#include "concurrency/eager_write_txn_manager.h"
#include "concurrency/ts_order_txn_manager.h"
#include "concurrency/ssi_txn_manager.h"
#include "concurrency/optimistic_rb_txn_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {
      case CONCURRENCY_TYPE_OPTIMISTIC:
        return OptimisticTxnManager::GetInstance();
      case CONCURRENCY_TYPE_PESSIMISTIC:
        return PessimisticTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SPECULATIVE_READ:
        return SpeculativeReadTxnManager::GetInstance();
      case CONCURRENCY_TYPE_EAGER_WRITE:
        return EagerWriteTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SSI:
        return SsiTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO:
        return TsOrderTxnManager::GetInstance();
      case CONCURRENCY_TYPE_OCC_RB:
        return OptimisticRbTxnManager::GetInstance();
      default:
        return OptimisticRbTxnManager::GetInstance();
    }
  }

  static void Configure(ConcurrencyType protocol,
                        IsolationLevelType level = ISOLATION_LEVEL_TYPE_FULL) {
    protocol_ = protocol;
    isolation_level_ = level;
  }

  static ConcurrencyType GetProtocol() { return protocol_; }

  static IsolationLevelType GetIsolationLevel() { return isolation_level_; }

 private:
  static ConcurrencyType protocol_;
  static IsolationLevelType isolation_level_;
};
}
}
