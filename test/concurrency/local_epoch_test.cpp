//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decentralized_epoch_manager_test.cpp
//
// Identification: test/concurrency/local_epoch_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/local_epoch.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class LocalEpochTests : public PelotonTest {};


TEST_F(LocalEpochTests, EpochCompareTest) {
  concurrency::EpochCompare comp;
  
  std::shared_ptr<concurrency::Epoch> epoch0(new concurrency::Epoch(10, 20));
  std::shared_ptr<concurrency::Epoch> epoch1(new concurrency::Epoch(10, 20));
  bool rt = comp(epoch0, epoch1);
  EXPECT_EQ(rt, false);

  std::shared_ptr<concurrency::Epoch> epoch2(new concurrency::Epoch(11, 21));
  std::shared_ptr<concurrency::Epoch> epoch3(new concurrency::Epoch(12, 20));
  rt = comp(epoch2, epoch3);
  EXPECT_EQ(rt, false);
}


TEST_F(LocalEpochTests, TransactionTest) {
  concurrency::LocalEpoch local_epoch(0);
  
  // a transaction enters epoch 10
  bool rt = local_epoch.EnterEpoch(10);
  EXPECT_EQ(rt, true);

  uint64_t max_eid = local_epoch.GetMaxCommittedEpochId(11);
  EXPECT_EQ(max_eid, 9);
  
  // a transaction enters epoch 15
  rt = local_epoch.EnterEpoch(15);
  EXPECT_EQ(rt, true);
  
  max_eid = local_epoch.GetMaxCommittedEpochId(18);
  EXPECT_EQ(max_eid, 9);
  
  // now only one transaction left
  local_epoch.ExitEpoch(10);

  max_eid = local_epoch.GetMaxCommittedEpochId(19);
  EXPECT_EQ(max_eid, 14);

  // now the lower bound is 14.
  // a transaction at epoch 12 must be rejected.
  rt = local_epoch.EnterEpoch(12);
  EXPECT_EQ(rt, false);

  // a read-only transaction can always succeed.
  local_epoch.EnterEpochRO(12);
  
  // consequently, the lower bound is dropped.
  max_eid = local_epoch.GetMaxCommittedEpochId(20);
  EXPECT_EQ(max_eid, 11);

  local_epoch.ExitEpoch(12);
  
  // now the lower bound is returned to 14.
  max_eid = local_epoch.GetMaxCommittedEpochId(21);
  EXPECT_EQ(max_eid, 14);

  local_epoch.ExitEpoch(15);

  // the last transaction has left.
  // then max_eid should be 25 - 1.
  max_eid = local_epoch.GetMaxCommittedEpochId(25);
  EXPECT_EQ(max_eid, 24);

  max_eid = local_epoch.GetMaxCommittedEpochId(30);
  EXPECT_EQ(max_eid, 29);
}


}  // End test namespace
}  // End peloton namespace

