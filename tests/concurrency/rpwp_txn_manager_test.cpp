//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pessimistic_transaction_manager_test.cpp
//
// Identification: tests/concurrency/rpwp_txn_manager_test.cpp
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

class RpwpTxnManagerTests : public PelotonTest {};

TEST_F(RpwpTxnManagerTests, Test) {
  concurrency::TransactionManagerFactory::Configure(CONCURRENCY_TYPE_RPWP);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
