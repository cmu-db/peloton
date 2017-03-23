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
        return TimestampOrderingTransactionManager::GetInstance(default_isolation_level_, conflict_avoidance_);

      default:
        return TimestampOrderingTransactionManager::GetInstance(default_isolation_level_, conflict_avoidance_);
    }
  }

  static void Configure(const ConcurrencyType protocol,
                        const IsolationLevelType level = IsolationLevelType::SERIALIZABLE, 
                        const ConflictAvoidanceType conflict = ConflictAvoidanceType::ABORT) {
    protocol_ = protocol;
    default_isolation_level_ = level;
    conflict_avoidance_ = conflict;
  }

  static ConcurrencyType GetProtocol() { return protocol_; }

  static IsolationLevelType GetDefaultIsolationLevel() { return default_isolation_level_; }

  static ConflictAvoidanceType GetConflictAvoidanceType() { return conflict_avoidance_; }

 private:
  static ConcurrencyType protocol_;
  static IsolationLevelType default_isolation_level_;
  static ConflictAvoidanceType conflict_avoidance_;
};
}
}