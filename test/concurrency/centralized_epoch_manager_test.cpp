//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager_test.cpp
//
// Identification: test/concurrency/centralized_epoch_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/epoch_manager_factory.h"
#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class CentralizedEpochManagerTests : public PelotonTest {};

TEST_F(CentralizedEpochManagerTests, Test) {
  concurrency::EpochManagerFactory::Configure(EpochType::CENTRALIZED_EPOCH);
  EXPECT_TRUE(true);
}

}  // End test namespace
}  // End peloton namespace
