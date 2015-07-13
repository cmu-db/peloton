/*-------------------------------------------------------------------------
 *
 * scheduler_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/scheduler/scheduler_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"

#include "harness.h"
#include "backend/scheduler/tbb_scheduler.h"
#include "backend/main/kernel.h"
#include "backend/catalog/catalog.h"

#include <sstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Scheduler Tests
//===--------------------------------------------------------------------===//

TEST(SchedulerTests, KernelTest) {
  auto kernel_func = &backend::Kernel::Handler;

  std::unique_ptr<scheduler::TBBTask> task1(new scheduler::TBBTask(reinterpret_cast<scheduler::handler>(kernel_func),
                                                                   const_cast<char*>("CREATE DATABASE TESTDB;")));

  std::unique_ptr<scheduler::TBBScheduler> tbb_scheduler(new scheduler::TBBScheduler());

  tbb_scheduler->Run(task1.get());

  // final wait
  tbb_scheduler->Wait();

}

} // End test namespace
} // End peloton namespace

