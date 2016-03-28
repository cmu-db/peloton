//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_transaction_manager_test.cpp
//
// Identification: tests/concurrency/optimistic_transaction_manager_test.cpp
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

class OptimisticTransactionManagerTests : public PelotonTest {};

TEST_F(OptimisticTransactionManagerTests, Test) {
  concurrency::TransactionManagerFactory::Configure(CONCURRENCY_TYPE_OCC);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
