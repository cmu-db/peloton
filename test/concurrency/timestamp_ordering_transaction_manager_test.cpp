//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager_test.cpp
//
// Identification: test/concurrency/timestamp_ordering_transaction_manager_test.cpp
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

class TimestampOrderingTransactionManagerTests : public PelotonTest {};

TEST_F(TimestampOrderingTransactionManagerTests, Test) {
  concurrency::TransactionManagerFactory::Configure(CONCURRENCY_TYPE_TIMESTAMP_ORDERING);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
