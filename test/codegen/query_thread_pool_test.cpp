//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_thread_pool_test.cpp
//
// Identification: test/codegen/query_thread_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_thread_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Query Thread Pool Test
//===--------------------------------------------------------------------===//

class QueryThreadPoolTests : public PelotonTest {};

int64_t func_add(int64_t *var, int64_t *var2) {
    *var = *var + *var2;
    return *var;
}

TEST_F(QueryThreadPoolTests, BasicTest) {
  QueryThreadPool thread_pool;
  thread_pool.Initialize(2, 1);

  std::atomic<int> counter(0);

  int var1 = 1;
  int var2 = 2;
  int var3 = 3;
  int var4 = 4;
  int var5 = 5;
  thread_pool.SubmitTask([](int *var, std::atomic<int> *counter) {
                           *var = *var + *var;
                           counter->fetch_add(1);
                         },
                         &var1, &counter);
  thread_pool.SubmitTask([](int *var, std::atomic<int> *counter) {
                           *var = *var - *var;
                           counter->fetch_add(1);
                         },
                         &var2, &counter);
  thread_pool.SubmitTask([](int *var, std::atomic<int> *counter) {
                           *var = *var * *var;
                           counter->fetch_add(1);
                         },
                         &var3, &counter);
  thread_pool.SubmitTask([](int *var, std::atomic<int> *counter) {
                           *var = *var / *var;
                           counter->fetch_add(1);
                         },
                         &var4, &counter);

  thread_pool.SubmitDedicatedTask([](int *var, std::atomic<int> *counter) {
                                    *var = *var / *var;
                                    counter->fetch_add(1);
                                  },
                                  &var5, &counter);
  int64_t var6 = 6;
  thread_pool.SubmitQueryTask(func_add, var6, var6);

  // Wait for all the test to finish
  while (counter.load() != 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }


  EXPECT_EQ(2, var1);
  EXPECT_EQ(0, var2);
  EXPECT_EQ(9, var3);
  EXPECT_EQ(1, var4);
  EXPECT_EQ(1, var5);

  thread_pool.Shutdown();
}

}  // End test namespace
}  // End peloton namespace

