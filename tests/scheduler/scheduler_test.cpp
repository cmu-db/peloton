//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// scheduler_test.cpp
//
// Identification: tests/scheduler/scheduler_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "harness.h"
#include "backend/main/kernel.h"

#include <sstream>
#include "backend/scheduler/tbb_scheduler.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Scheduler Tests
//===--------------------------------------------------------------------===//

TEST(SchedulerTests, KernelTest) {
  auto kernel_func = &backend::Kernel::Handler;

  std::unique_ptr<scheduler::TBBScheduler> tbb_scheduler(
      new scheduler::TBBScheduler());

  tbb_scheduler->Run(reinterpret_cast<scheduler::handler>(kernel_func),
                     const_cast<char *>("CREATE DATABASE TESTDB;"));

  // final wait
  tbb_scheduler->Wait();
}

}  // End test namespace
}  // End peloton namespace
