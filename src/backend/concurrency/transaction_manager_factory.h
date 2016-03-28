//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager_factory.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/optimistic_transaction_manager.h"
#include "backend/concurrency/pessimistic_transaction_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol) {
      case CONCURRENCY_TYPE_OCC:
        return OptimisticTransactionManager::GetInstance();
      case CONCURRENCY_TYPE_2PL:
        return PessimisticTransactionManager::GetInstance();
      default:
        return OptimisticTransactionManager::GetInstance();
    }
  }

  static void Configure(ConcurrencyType protocol_,
                        IsolationLevelType level_ = ISOLATION_LEVEL_TYPE_FULL) {
    protocol = protocol_;
    isolation_level = level_;
  }

  static ConcurrencyType GetProtocol() { return protocol; }

  static IsolationLevelType GetIsolationLevel() { return isolation_level; }

 private:
  static ConcurrencyType protocol;
  static IsolationLevelType isolation_level;
};
}
}