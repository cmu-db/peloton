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
// TransactionContext Tests
//===--------------------------------------------------------------------===//

class LocalEpochTests : public PelotonTest {};


TEST_F(LocalEpochTests, EpochCompareTest) {
  concurrency::EpochCompare comp;
  
  std::shared_ptr<concurrency::Epoch> epoch0(new concurrency::Epoch(10, 20));
  std::shared_ptr<concurrency::Epoch> epoch1(new concurrency::Epoch(10, 20));
  bool rt = comp(epoch0, epoch1);
  EXPECT_FALSE(rt);

  std::shared_ptr<concurrency::Epoch> epoch2(new concurrency::Epoch(11, 21));
  std::shared_ptr<concurrency::Epoch> epoch3(new concurrency::Epoch(12, 20));
  rt = comp(epoch2, epoch3);
  EXPECT_FALSE(rt);
}


TEST_F(LocalEpochTests, TransactionTest) {
  concurrency::LocalEpoch local_epoch(0);
  
  // a transaction enters epoch 10
  bool rt = local_epoch.EnterEpoch(10, TimestampType::READ);
  EXPECT_TRUE(rt);

  uint64_t max_eid = local_epoch.GetExpiredEpochId(11);
  EXPECT_EQ(max_eid, 9);
  
  // a transaction enters epoch 15
  rt = local_epoch.EnterEpoch(15, TimestampType::READ);
  EXPECT_TRUE(rt);
  
  max_eid = local_epoch.GetExpiredEpochId(18);
  EXPECT_EQ(max_eid, 9);
  
  // now only one transaction left
  local_epoch.ExitEpoch(10);

  max_eid = local_epoch.GetExpiredEpochId(19);
  EXPECT_EQ(max_eid, 14);

  // now the lower bound is 14.
  // a transaction at epoch 12 must be rejected.
  rt = local_epoch.EnterEpoch(12, TimestampType::READ);
  EXPECT_FALSE(rt);

  // a read-only transaction can always succeed.
  local_epoch.EnterEpoch(12, TimestampType::SNAPSHOT_READ);
  
  // consequently, the lower bound is dropped.
  max_eid = local_epoch.GetExpiredEpochId(20);
  EXPECT_EQ(max_eid, 11);

  local_epoch.ExitEpoch(12);
  
  // now the lower bound is returned to 14.
  max_eid = local_epoch.GetExpiredEpochId(21);
  EXPECT_EQ(max_eid, 14);

  local_epoch.ExitEpoch(15);

  // the last transaction has left.
  // then max_eid should be 25 - 1.
  max_eid = local_epoch.GetExpiredEpochId(25);
  EXPECT_EQ(max_eid, 24);

  max_eid = local_epoch.GetExpiredEpochId(30);
  EXPECT_EQ(max_eid, 29);
}


}  // namespace test
}  // namespace peloton

