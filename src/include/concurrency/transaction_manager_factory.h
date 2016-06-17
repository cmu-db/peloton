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

#include "concurrency/ts_order_txn_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {

      case CONCURRENCY_TYPE_TO:
        return TsOrderTxnManager::GetInstance();

      default:
        return TsOrderTxnManager::GetInstance();
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
