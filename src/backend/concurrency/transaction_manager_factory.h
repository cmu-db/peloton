//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager_factory.h
//
// Identification: src/backend/concurrency/transaction_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/rowo_txn_manager.h"
#include "backend/concurrency/rpwp_txn_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol) {
      case CONCURRENCY_TYPE_OCC:
        return RowoTxnManager::GetInstance();
      case CONCURRENCY_TYPE_2PL:
        return RpwpTxnManager::GetInstance();
      default:
        return RowoTxnManager::GetInstance();
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