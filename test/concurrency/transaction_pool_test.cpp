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
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionPoolTests : public PelotonTest {};

TEST_F(TransactionPoolTests, ConstructionTest) {
  
  concurrency::TransactionPool txn_pool(10);

  EXPECT_TRUE(true);
}


TEST_F(TransactionPoolTests, AcquireTest) {
  
  concurrency::TransactionPool txn_pool(3);

  concurrency::Transaction *txn1 = nullptr;
  txn_pool.AcquireTransaction(txn1);

  concurrency::Transaction *txn2 = nullptr;
  txn_pool.AcquireTransaction(txn2);

  concurrency::Transaction *txn3 = nullptr;
  txn_pool.AcquireTransaction(txn3);

  concurrency::Transaction *txn4 = nullptr;
  bool rt = txn_pool.TryAcquireTransaction(txn4);
  EXPECT_TRUE(rt == false);

  txn_pool.ReleaseTransaction(txn1);
  txn_pool.ReleaseTransaction(txn2);
  txn_pool.ReleaseTransaction(txn3);

  rt = txn_pool.TryAcquireTransaction(txn4);
  EXPECT_TRUE(rt == true);
  txn_pool.ReleaseTransaction(txn4);

  EXPECT_TRUE(true);
}


}  // End test namespace
}  // End peloton namespace
