//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// speculative_read_txn_manager_test.cpp
//
// Identification: test/concurrency/speculative_read_txn_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class SpeculativeReadTxnManagerTests : public PelotonTest {};

TEST_F(SpeculativeReadTxnManagerTests, Test) {
  concurrency::TransactionManagerFactory::Configure(
      CONCURRENCY_TYPE_SPECULATIVE_READ);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
