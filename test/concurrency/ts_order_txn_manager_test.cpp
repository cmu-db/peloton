//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_txn_manager_test.cpp
//
// Identification: tests/concurrency/ts_order_txn_manager_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TsOrderTxnManagerTests : public PelotonTest {};

TEST_F(TsOrderTxnManagerTests, Test) {
  concurrency::TransactionManagerFactory::Configure(CONCURRENCY_TYPE_TO);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
