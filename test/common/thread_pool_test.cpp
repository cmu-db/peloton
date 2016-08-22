//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool_test.cpp
//
// Identification: test/common/thread_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/thread_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Thread Pool Test
//===--------------------------------------------------------------------===//

class ThreadPoolTests : public PelotonTest {};

TEST_F(ThreadPoolTests, BasicTest) {
  ThreadPool thread_pool;
  thread_pool.Initialize(2);

  int var1 = 1;
  int var2 = 2;
  int var3 = 3;
  int var4 = 4;
  thread_pool.SubmitTask([](int *var) { *var = *var + *var; }, &var1);
  thread_pool.SubmitTask([](int *var) { *var = *var - *var; }, &var2);
  thread_pool.SubmitTask([](int *var) { *var = *var * *var; }, &var3);
  thread_pool.SubmitTask([](int *var) { *var = *var / *var; }, &var4);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_EQ(2, var1);
  EXPECT_EQ(0, var2);
  EXPECT_EQ(9, var3);
  EXPECT_EQ(1, var4);
}

}  // End test namespace
}  // End peloton namespace
