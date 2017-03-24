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
  cid_t txn_id = epoch_manager.EnterEpoch(0, false);

  // we should expect that the tail is 1.
  uint64_t tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(1, tail_epoch_id);

  epoch_manager.Reset(3);

  // we should expect that the tail is 1.
  tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(1, tail_epoch_id);
  
  epoch_manager.ExitEpoch(0, txn_id);

  epoch_manager.Reset(4);

  tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(3, tail_epoch_id);

  // deregister a thread.
  epoch_manager.DeregisterThread(0);
}


TEST_F(DecentralizedEpochManagerTests, MultipleThreadsTest) {

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  // originally, the global epoch is 1.
  epoch_manager.Reset(1);

  // register three threads.
  epoch_manager.RegisterThread(0);

  epoch_manager.RegisterThread(1);

  epoch_manager.RegisterThread(2); // this is an idle thread.

  epoch_manager.Reset(2);

  // create a transaction at epoch 2.
  cid_t txn_id1 = epoch_manager.EnterEpoch(0, false);

  // we should expect that the tail is 1.
  uint64_t tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(1, tail_epoch_id);

  epoch_manager.Reset(3);

  // create a transaction at epoch 3.
  cid_t txn_id2 = epoch_manager.EnterEpoch(1, false);

  // we should expect that the tail is 1.
  tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(1, tail_epoch_id);
  
  epoch_manager.ExitEpoch(0, txn_id1);

  epoch_manager.Reset(5);

  tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  // we still have one thread running at epoch 3.
  EXPECT_EQ(2, tail_epoch_id);

  epoch_manager.ExitEpoch(1, txn_id2);

  tail_epoch_id = epoch_manager.GetMaxCommittedEpochId();

  EXPECT_EQ(4, tail_epoch_id);


  // deregister two threads.
  epoch_manager.DeregisterThread(0);

  epoch_manager.DeregisterThread(1);

  epoch_manager.DeregisterThread(2);
}


}  // End test namespace
}  // End peloton namespace

