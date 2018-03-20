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
// TransactionContext Tests
//===--------------------------------------------------------------------===//

class DecentralizedEpochManagerTests : public PelotonTest {};


TEST_F(DecentralizedEpochManagerTests, Test) {
  concurrency::EpochManagerFactory::Configure(EpochType::DECENTRALIZED_EPOCH);
  EXPECT_TRUE(true);
}


TEST_F(DecentralizedEpochManagerTests, SingleThreadTest) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset();

  // originally, the global epoch is 1.
  epoch_manager.SetCurrentEpochId(1);

  // register a thread.
  epoch_manager.RegisterThread(0);

  epoch_manager.SetCurrentEpochId(2);

  // create a transaction at epoch 2.
  cid_t txn_id = epoch_manager.EnterEpoch(0, TimestampType::READ);

  eid_t epoch_id = txn_id >> 32;

  // we should expect that the tail is 1.
  uint64_t tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, tail_epoch_id);

  epoch_manager.SetCurrentEpochId(3);

  // we should expect that the tail is 1.
  tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, tail_epoch_id);
  
  epoch_manager.ExitEpoch(0, epoch_id);

  epoch_manager.SetCurrentEpochId(4);

  tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(3, tail_epoch_id);

  // deregister a thread.
  epoch_manager.DeregisterThread(0);
}


TEST_F(DecentralizedEpochManagerTests, MultipleThreadsTest) {

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset();

  // originally, the global epoch is 1.
  epoch_manager.SetCurrentEpochId(1);

  // register three threads.
  epoch_manager.RegisterThread(0);

  epoch_manager.RegisterThread(1);

  epoch_manager.RegisterThread(2); // this is an idle thread.

  epoch_manager.SetCurrentEpochId(2);

  // create a transaction at epoch 2.
  cid_t txn_id1 = epoch_manager.EnterEpoch(0, TimestampType::READ);

  eid_t epoch_id1 = txn_id1 >> 32;

  // we should expect that the tail is 1.
  uint64_t tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, tail_epoch_id);

  epoch_manager.SetCurrentEpochId(3);

  // create a transaction at epoch 3.
  cid_t txn_id2 = epoch_manager.EnterEpoch(1, TimestampType::READ);

  eid_t epoch_id2 = txn_id2 >> 32;

  // we should expect that the tail is 1.
  tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(1, tail_epoch_id);
  
  epoch_manager.ExitEpoch(0, epoch_id1);

  epoch_manager.SetCurrentEpochId(5);

  tail_epoch_id = epoch_manager.GetExpiredEpochId();

  // we still have one thread running at epoch 3.
  EXPECT_EQ(2, tail_epoch_id);

  epoch_manager.ExitEpoch(1, epoch_id2);

  tail_epoch_id = epoch_manager.GetExpiredEpochId();

  EXPECT_EQ(4, tail_epoch_id);


  // deregister two threads.
  epoch_manager.DeregisterThread(0);

  epoch_manager.DeregisterThread(1);

  epoch_manager.DeregisterThread(2);
}


}  // namespace test
}  // namespace peloton

