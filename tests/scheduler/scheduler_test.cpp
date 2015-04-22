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
#include "parser/parser.h"

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

// ORIGINAL METHOD

struct hello_args {
  char* s;
  int i;
};

void *hello_task(void* params) {
  struct hello_args* p = (struct hello_args*) params;

  printf("Hello %s, number %d\n", p->s, p->i);
  usleep(100);

  return nullptr;
}

TEST(SchedulerTests, SchedulerTest) {

  int i, count = 10;
  struct hello_args hp[count];
  for (i = 0; i < count; i++) {
    hp[i].s = "World";
    hp[i].i = i;
    scheduler::Scheduler::GetInstance().AddTask(&hello_task, &hp[i]);
  }

  struct hello_args hp2[count];
  for (i = 0; i < count; i++) {
    hp2[i].s = "World 2";
    hp2[i].i = i;
    scheduler::Scheduler::GetInstance().AddTask(&hello_task, &hp2[i], scheduler::TASK_PRIORTY_TYPE_HIGH);
  }

  // final wait
  scheduler::Scheduler::GetInstance().Wait();
}

TEST(SchedulerTests, ParseTest) {

  parser::SQLStatementList* (*parser_func)(const char*) = &parser::Parser::ParseSQLString;

  scheduler::Task *task = scheduler::Scheduler::GetInstance().AddTask((void *(*)(void*)) parser_func,
                                              (void *) "SELECT * FROM ORDERS;");

  // final wait
  scheduler::Scheduler::GetInstance().Wait();

  std::cout << "Task :: " << task << "\n";

  parser::SQLStatementList* output = (parser::SQLStatementList*) task->GetOuput();

  if(output != nullptr) {
    std::cout << (*output);
  }

}

} // End test namespace
} // End nstore namespace

