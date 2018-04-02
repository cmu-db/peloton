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

      case ProtocolType::TIMESTAMP_ORDERING:
        return TimestampOrderingTransactionManager::GetInstance(protocol_, isolation_level_, conflict_avoidance_);

      default:
        return TimestampOrderingTransactionManager::GetInstance(protocol_, isolation_level_, conflict_avoidance_);
    }
  }

  static void Configure(const ProtocolType protocol,
                        const IsolationLevelType isolation = IsolationLevelType::SERIALIZABLE, 
                        const ConflictAvoidanceType conflict = ConflictAvoidanceType::ABORT) {
    protocol_ = protocol;
    isolation_level_ = isolation;
    conflict_avoidance_ = conflict;
  }

  /**
   * @brief      Gets the protocol.
   *
   * @return     The protocol.
   */
  static ProtocolType GetProtocol() { return protocol_; }

  /**
   * @brief      Gets the isolation level.
   *
   * @return     The isolation level.
   */
  static IsolationLevelType GetIsolationLevel() { return isolation_level_; }

  /**
   * @brief      Gets the conflict avoidance type.
   *
   * @return     The conflict avoidance type.
   */
  static ConflictAvoidanceType GetConflictAvoidanceType() { return conflict_avoidance_; }

 private:
  static ProtocolType protocol_;
  static IsolationLevelType isolation_level_;
  static ConflictAvoidanceType conflict_avoidance_;
};
}
}