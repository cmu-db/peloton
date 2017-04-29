//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool_test.cpp
//
// Identification: test/task/worker_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include "gtest/gtest.h"
#include "common/logger.h"
#include "common/harness.h"
#include "task/worker_pool.h"

namespace peloton{
namespace test{

class WorkerPoolTest : public PelotonTest {};

static void blah(UNUSED_ATTRIBUTE void* args){
  sleep(2);
  LOG_INFO("Yingun smells...");
}

TEST_F(WorkerPoolTest, BasicTest){
  task::WorkerPool pool(2, 8);
  pool.SubmitTask(blah, nullptr);
  pool.SubmitTask(blah, nullptr);
  pool.SubmitTask(blah, nullptr);
  pool.Shutdown();


}

} //test
} //peloton
