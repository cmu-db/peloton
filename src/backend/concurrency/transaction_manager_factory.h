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

namespace peloton {
  namespace concurrency {
    class TransactionManagerFactory {
    public:
      static TransactionManager &GetInstance() {
        return OptimisticTransactionManager::GetInstance();
      }
    };
  }
}