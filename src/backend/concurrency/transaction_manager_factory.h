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
#include "backend/concurrency/spec_rowo_txn_manager.h"
#include "backend/concurrency/ssi_txn_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {
      case CONCURRENCY_TYPE_ROWO:
        return RowoTxnManager::GetInstance();
      case CONCURRENCY_TYPE_RPWP:
        return RpwpTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SPEC:
        return SpecRowoTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SSI:
        return SsiTxnManager::GetInstance();
      default:
        return RowoTxnManager::GetInstance();
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