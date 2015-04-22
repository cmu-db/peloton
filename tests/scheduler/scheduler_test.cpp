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
#include "scheduler/traffic_cop.h"
#include "scheduler/scheduler.h"
#include "backend/kernel.h"

#include <sstream>

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Scheduler Tests
//===--------------------------------------------------------------------===//

TEST(SchedulerTests, TrafficCopTest) {

  std::istringstream stream(
      "1 0 process txn 1 query 1 \n"
      "1 100 process txn 1 query 2 \n"
      "1 0 process txn 2 query 1 \n"
      "1 100 process txn 1 query 3 \n"
      "1 200 process txn 2 query 2 \n"
      "1 200 process txn 2 query 3 \n"
      "1 200 commit txn 2 \n"
      "1 100 rollback txn 1 \n"
      "2 100 response txn 1 \n"
      "3 0 stop the server"
  );

  std::cin.rdbuf(stream.rdbuf());

  // Start traffic cop
  scheduler::TrafficCop::GetInstance().Execute();
}

TEST(SchedulerTests, KernelTest) {

  ResultType (*kernel_func)(const char*) = &backend::Kernel::Handler;

  scheduler::Scheduler::GetInstance().AddTask((ResultType (*)(void*)) kernel_func,
                                              (void *) "SELECT * FROM ORDERS;",
                                              TASK_PRIORTY_TYPE_HIGH);

  // final wait
  scheduler::Scheduler::GetInstance().Wait();

}

} // End test namespace
} // End nstore namespace

