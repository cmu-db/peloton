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

#include <sstream>

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Scheduler Tests
//===--------------------------------------------------------------------===//

TEST(SchedulerTests, BasicTest) {

  std::istringstream stream(
      "1 process query 1 \n"
      "1 process query 2 \n"
      "2 stop the server"
  );

  std::cin.rdbuf(stream.rdbuf());

  // Start traffic cop
  scheduler::TrafficCop::Execute();
}

} // End test namespace
} // End nstore namespace

