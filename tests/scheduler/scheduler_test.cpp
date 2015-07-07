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
  ResultType (*kernel_func)(const char*) = &backend::Kernel::Handler;

  std::unique_ptr<scheduler::TBBTask> task1(new scheduler::TBBTask(kernel_func,
                                                                   "CREATE DATABASE TESTDB;"));

  auto& tbb_scheduler = scheduler::TBBScheduler::GetInstance();

  tbb_scheduler.AddTask(task1.get());

  // final wait
  tbb_scheduler.Execute();

}

} // End test namespace
} // End peloton namespace

