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

#include "common/harness.h"
#include "common/logger.h"
#include "common/macros.h"
#include "task/worker.h"
#include "task/task.h"
#include <iostream>
#include <thread>
#include <cstdio>

namespace peloton {
namespace test {

class WorkerPoolTests : public PelotonTest {};
/*
class MasterThread {
 public:
  MasterThread(const size_t queue_size, const size_t pool_size):
};*/

void printString(std::vector<void *> args) {
  std::string *sp = static_cast<std::string *>(*args.begin());
  std::string str = *sp;
  std::thread::id id = std::this_thread::get_id();
  LOG_INFO("thread %x get %s", id, str.c_str());
}

void masterCallback() {
  LOG_INFO("master activate");
}

TEST_F(WorkerPoolTests, OneWorkerTest) {
  LOG_INFO("master starts");
  const size_t sz = 10;
  peloton::task::TaskQueue tq(sz);
  peloton::task::WorkerPool wp(1, &tq);

  std::vector<void *> params1;
  std::string p1 = "string1";
  params1.push_back((void*)&p1);

  std::vector<void *> params2;
  std::string p2 = "string2";
  params2.push_back((void *)&p2);

  std::vector<shared_ptr<Task *>> task_v;
  peloton::task::Task task1(func1, params1);
  task_v.push_back(shared_ptr<Task>(task1));
  peloton::task::Task task2(func1, params2);
  task_v.push_back(shared_ptr<Task>(task2));

  tq.SubmitTaskBatch(task_v);

  LOG_INFO("master finishes");
  wp.Shutdown();
}

TEST_F(WorkerPoolTests, MultiWorkerTest) {
  LOG_INFO("master starts");
  const size_t sz = 10;
  peloton::task::TaskQueue tq(sz);
  peloton::task::WorkerPool wp(1, &tq);

  std::vector<void *> params1;
  std::string p1 = "string1";
  params1.push_back((void*)&p1);

  std::vector<void *> params2;
  std::string p2 = "string2";
  params2.push_back((void *)&p2);

  std::vector<shared_ptr<Task> task_v;
  peloton::task::Task task1(func1, params1);
  task_v.push_back(shared_ptr<Task>(task1));
  peloton::task::Task task2(func1, params2);
  task_v.push_back(shared_ptr<Task>(task2));

  tq.SubmitTaskBatch(task_v);

  LOG_INFO("master finishes");
  wp.Shutdown();
}
/*
TEST_F(WorkerPoolTests, ActivateTest) {
  LOG_INFO("master starts");
  const size_t sz = 10;
  peloton::task::TaskQueue tq(sz);
  peloton::task::WorkerPool wp(1, &tq);

  std::vector<void *> params1;
  std::string p1 = "func1";
  params1.push_back((void*)&p1);

  std::vector<void *> params2;
  std::string p2 = "func2";
  params2.push_back((void *)&p2);

  std::vector<shared_ptr<Task>> task_v;
  peloton::task::Task task1(func1, params1);
  task_v.push_back(shared_ptr<Task>(task1));
  peloton::task::Task task2(func1, params2);
  task_v.push_back(shared_ptr<Task>(task2));

  tq.SubmitTaskBatch(task_v);

  LOG_INFO("master finishes");
  wp.Shutdown();
}*/

} // end of test
} // end of peloton