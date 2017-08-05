//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// threadpool.h
//
// Identification: src/networking/network_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <condition_variable>
#include <vector>

#include "common/harness.h"
#include "threadpool/mono_queue_pool.h"


namespace peloton {
namespace test {

class MonoQueuePoolTests : public PelotonTest {};
std::mutex m;
std::condition_variable cv;

void CallBackFuncSync(UNUSED_ATTRIBUTE void *arg) {
  LOG_DEBUG("Start call back");
  int* num = (int*)arg;
  usleep(1000000);
  (*num)++;
  LOG_DEBUG("Finish call back");
}

void CallBackFuncAsync(UNUSED_ATTRIBUTE void *arg) {
  LOG_DEBUG("Start call back");
  std::unique_lock <std::mutex> lock(m);
  int *num = (int *) arg;
  (*num)++;
  cv.notify_all();
  LOG_DEBUG("Finish call back");
}

TEST_F(MonoQueuePoolTests, SyncExecuteTest) {
  LOG_DEBUG("Start synchronous execution test");
  int number = 1;
  threadpool::MonoQueuePool::GetInstance().ExecuteSync(CallBackFuncSync, &number);
  EXPECT_EQ(number, 2);
  LOG_DEBUG("Finish synchronous execution test");

}


TEST_F(MonoQueuePoolTests, AsyncExecuteTest) {
  LOG_DEBUG("Start asynchronous execution");

  int number = 1;
  std::unique_lock <std::mutex> lock(m);
  threadpool::MonoQueuePool::GetInstance().ExecuteAsync(CallBackFuncAsync, &number);
  EXPECT_EQ(number, 1);
  cv.wait(lock);
  EXPECT_EQ(number, 2);
  LOG_DEBUG("Finish synchronous execution test");
}
}
}