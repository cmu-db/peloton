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
#include "backend/concurrency/ssi_transaction_manager.h"
namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {
      case CONCURRENCY_TYPE_OCC:
        return OptimisticTransactionManager::GetInstance();
      case CONCURRENCY_TYPE_2PL:
        return PessimisticTransactionManager::GetInstance();
      case CONCURRENCY_TYPE_SSI:
        return SsiTransactionManager::GetInstance();
      default:
        return OptimisticTransactionManager::GetInstance();
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