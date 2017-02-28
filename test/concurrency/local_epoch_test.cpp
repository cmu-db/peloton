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


#include "concurrency/epoch_manager_factory.h"
#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class LocalEpochTests : public PelotonTest {};


TEST_F(LocalEpochTests, EpochCompareTest) {
  concurrency::Epoch epoch0(10, 20);
  concurrency::Epoch epoch1(10, 20);
  bool rt = EpochCompare(epoch0, epoch1);
  EXPECT_EQ(rt, false);

  concurrency::Epoch epoch2(11, 21);
  concurrency::Epoch epoch3(12, 20);
  rt = EpochCompare(epoch2, epoch3);
  EXPECT_EQ(rt, true);
}


TEST_F(LocalEpochTests, SingleThreadTest) {


}


TEST_F(LocalEpochTests, MultipleThreadsTest) {


}


}  // End test namespace
}  // End peloton namespace

