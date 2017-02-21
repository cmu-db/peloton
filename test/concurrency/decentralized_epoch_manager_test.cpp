//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decentralized_epoch_manager_test.cpp
//
// Identification: test/concurrency/decentralized_epoch_manager_test.cpp
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

class DecentralizedEpochManagerTests : public PelotonTest {};


TEST_F(DecentralizedEpochManagerTests, Test) {
  concurrency::EpochManagerFactory::Configure(EpochType::DECENTRALIZED_EPOCH);
  EXPECT_TRUE(true);
}


TEST_F(DecentralizedEpochManagerTests, SingleThreadTest) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  // originally, the global epoch is 1.
  epoch_manager.Reset(1);

  // register a thread.
  epoch_manager.RegisterThread(0);

  epoch_manager.Reset(2);

  // create a transaction at epoch 2.
  cid_t txn_id = epoch_manager.EnterEpochD(0);

  // we should expect that the tail is 1.
  uint64_t tail_epoch_id = epoch_manager.GetTailEpochId();

  EXPECT_EQ(1, tail_epoch_id);

  epoch_manager.Reset(3);

  // we should expect that the tail is 1.
  tail_epoch_id = epoch_manager.GetTailEpochId();

  EXPECT_EQ(1, tail_epoch_id);
  
  epoch_manager.ExitEpochD(0, txn_id);

  epoch_manager.Reset(4);

  tail_epoch_id = epoch_manager.GetTailEpochId();

  EXPECT_EQ(3, tail_epoch_id);

}


TEST_F(DecentralizedEpochManagerTests, MultipleThreadTest) {
}


}  // End test namespace
}  // End peloton namespace
