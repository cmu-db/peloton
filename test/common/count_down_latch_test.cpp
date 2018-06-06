//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down_latch_test.cpp
//
// Identification: test/common/count_down_latch_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/synchronization/count_down_latch.h"

#include "common/harness.h"

namespace peloton {
namespace test {

class CountDownLatchTest : public PelotonTest {};

TEST_F(CountDownLatchTest, ZeroCountTest) {
  common::synchronization::CountDownLatch latch(0);
  EXPECT_TRUE(latch.Await(0));
  EXPECT_TRUE(latch.Await(1000));
  LaunchParallelTest(4, [&latch](uint64_t) { latch.CountDown(); });
  EXPECT_EQ(0, latch.GetCount());
}

TEST_F(CountDownLatchTest, FinishBeforeAwaitTest) {
  uint32_t count = 4;
  common::synchronization::CountDownLatch latch(count);

  // Finish latch first
  LaunchParallelTest(count, [&latch](uint64_t) { latch.CountDown(); });

  // Wait should succeed immediately
  EXPECT_TRUE(latch.Await(0));
  EXPECT_TRUE(latch.Await(1000));
}

TEST_F(CountDownLatchTest, SimpleLatchTest) {
  // Create latch with count 4
  uint32_t count = 4;
  common::synchronization::CountDownLatch latch(count);

  // Awaiting now should return false, indicating the latch is not finished
  EXPECT_FALSE(latch.Await(1000));
  EXPECT_EQ(count, latch.GetCount());

  // Launch two threads to countdown
  LaunchParallelTest(2, [&latch](uint64_t) { latch.CountDown(); });

  // Latch still not complete
  EXPECT_FALSE(latch.Await(1000));
  EXPECT_EQ(2, latch.GetCount());

  // Finish latch now
  LaunchParallelTest(2, [&latch](uint64_t) { latch.CountDown(); });

  // Latch should be complete
  EXPECT_TRUE(latch.Await(1000));
  EXPECT_EQ(0, latch.GetCount());
}

TEST_F(CountDownLatchTest, OvercommitLatchTest) {
  // Create latch with count 4, but spawn more than 4*2=8 threads
  uint32_t count = 4;
  common::synchronization::CountDownLatch latch(count);

  // Awaiting now should return false, indicating the latch is not finished
  EXPECT_FALSE(latch.Await(1000));
  EXPECT_EQ(count, latch.GetCount());

  // Sleep for 10ms, then pummel the latch. Some threads should see count zero.
  LaunchParallelTest(count * 2, [&latch](uint64_t) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    latch.CountDown();
  });

  EXPECT_TRUE(latch.Await(1000));
  EXPECT_EQ(0, latch.GetCount());
}

}  // namespace test
}  // namespace peloton