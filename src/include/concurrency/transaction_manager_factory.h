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

#include "concurrency/timestamp_ordering_transaction_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {

      case ConcurrencyType::TIMESTAMP_ORDERING:
        return TimestampOrderingTransactionManager::GetInstance();

      default:
        return TimestampOrderingTransactionManager::GetInstance();
    }
  }

  static void Configure(ConcurrencyType protocol,
                        IsolationLevelType level = IsolationLevelType::FULL) {
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