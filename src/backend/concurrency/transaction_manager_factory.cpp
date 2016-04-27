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

#include "transaction_manager_factory.h"

namespace peloton {
namespace concurrency {
ConcurrencyType TransactionManagerFactory::protocol_ =
    CONCURRENCY_TYPE_PESSIMISTIC;
IsolationLevelType TransactionManagerFactory::isolation_level_ =
    ISOLATION_LEVEL_TYPE_FULL;
}
}
