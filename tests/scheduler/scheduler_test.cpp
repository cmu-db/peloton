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

  std::unique_ptr<scheduler::AbstractTask> task1(new scheduler::AbstractTask(kernel_func,
                                                                   (void *)
                                                                   "CREATE DATABASE TEST;"
                                                                   "CREATE TABLE T (T_ID INTEGER NOT NULL,"
                                                                   "DESCRIPTION VARCHAR(300),"
                                                                   "PRIMARY KEY (T_ID)"
                                                                   ");"));

  scheduler::TBBScheduler::GetInstance().AddTask(task1.get());

  // final wait
  scheduler::TBBScheduler::GetInstance().Execute();
}

} // End test namespace
} // End peloton namespace

