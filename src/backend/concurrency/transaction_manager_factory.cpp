//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager_factory.cpp
//
// Identification: src/backend/concurrency/transaction_manager_factory.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "transaction_manager_factory.h"

namespace peloton {
namespace concurrency {
ConcurrencyType TransactionManagerFactory::protocol = CONCURRENCY_TYPE_OCC;
IsolationLevelType TransactionManagerFactory::isolation_level =
    ISOLATION_LEVEL_TYPE_FULL;
}
}
