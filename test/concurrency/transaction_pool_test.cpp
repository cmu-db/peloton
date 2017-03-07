//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_pool_test.cpp
//
// Identification: test/concurrency/transaction_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/testing_transaction_util.h"
#include "concurrency/transaction_pool.h"
#include "common/harness.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// TransactionPool Tests
//===--------------------------------------------------------------------===//

class TransactionPoolTests : public PelotonTest {};


TEST_F(TransactionPoolTests, Test) {
  
  concurrency::TransactionPool::Configure(4, 2);

  auto &txn_pool = concurrency::TransactionPool::GetInstance();

  concurrency::Transaction *txn1 = nullptr;
  txn_pool.AcquireTransaction(txn1, 0);
  EXPECT_EQ(txn1->GetTransactionId(), 0);
  
  concurrency::Transaction *txn2 = nullptr;
  txn_pool.AcquireTransaction(txn2, 0);
  EXPECT_EQ(txn2->GetTransactionId(), 1);

  concurrency::Transaction *txn3 = nullptr;
  txn_pool.AcquireTransaction(txn3, 1);
  EXPECT_EQ(txn3->GetTransactionId(), 2);

  concurrency::Transaction *txn4 = nullptr;
  txn_pool.AcquireTransaction(txn4, 1);
  EXPECT_EQ(txn4->GetTransactionId(), 3);
  
  concurrency::Transaction *txn5 = nullptr;
  bool rt = txn_pool.TryAcquireTransaction(txn5, 0);
  EXPECT_EQ(rt, false);

  txn_pool.ReleaseTransaction(txn1);
  txn_pool.ReleaseTransaction(txn2);
  txn_pool.ReleaseTransaction(txn3);
  txn_pool.ReleaseTransaction(txn4);

  rt = txn_pool.TryAcquireTransaction(txn5, 0);
  EXPECT_EQ(rt, true);
  
  txn_pool.ReleaseTransaction(txn5);
}


}  // End test namespace
}  // End peloton namespace
