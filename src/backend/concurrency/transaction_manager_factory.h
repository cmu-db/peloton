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
  static TransactionManager &GetInstance(
      ConcurrencyType concurrency_type = CONCURRENCY_TYPE_2PL) {
    switch (concurrency_type) {
      case CONCURRENCY_TYPE_OCC:
        return OptimisticTransactionManager::GetInstance();
      case CONCURRENCY_TYPE_2PL:
        return PessimisticTransactionManager::GetInstance();
      default:
        return OptimisticTransactionManager::GetInstance();
    }
  }
};
}
}