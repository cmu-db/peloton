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

// ORIGINAL METHOD

struct hello_args {
  char* s;
  int i;
};

void hello_task(void* params) {
  struct hello_args* p = (struct hello_args*) params;

  printf("Hello %s, number %d\n", p->s, p->i);
  //sleep(1);
}

TEST(SchedulerTests, MoreTests) {
  scheduler::Scheduler *mng = NULL;

  if (mng == NULL)
    mng = new scheduler::Scheduler();

  int i, count = 10;
  struct hello_args hp[count];
  for (i = 0; i < count; i++) {
    hp[i].s = "World";
    hp[i].i = i;
    mng->AddTask(&hello_task, &hp[i]);
  }

  mng->Wait();

  struct hello_args hp2[count];
  for (i = 0; i < count; i++) {
    hp2[i].s = "World";
    hp2[i].i = i;
    mng->AddTask(&hello_task, &hp2[i]);
  }

  mng->Wait();

  delete mng;
}

} // End test namespace
} // End nstore namespace

