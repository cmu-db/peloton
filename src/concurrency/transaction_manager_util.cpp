//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager_util.cpp
//
// Identification: src/concurrency/transaction_manager_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/transaction_manager_util.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace concurrency {

concurrency::Transaction* BeginTransaction() {
  
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  
  return txn_manager.BeginTransaction();

}

void CommitTransaction(concurrency::Transaction *transaction) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  return txn_manager.CommitTransaction(transaction);
}

void AbortTransaction(concurrency::Transaction *transaction) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  return txn_manager.AbortTransaction(transaction);

}

}
}
