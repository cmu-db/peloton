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

void func1(std::vector<void *> args) {
  std::string *sp = static_cast<std::string *>(*args.begin());
  std::string str = *sp;
  std::thread::id id = std::this_thread::get_id();
  std::cout << "thread " << id << ", get " << str.c_str() << std::endl;
}

void hear() {
  std::cout << "master activate";
}

TEST_F(WorkerPoolTests, OneWorkerTest) {
  std::cout << "master, start" <<
  std::endl;
  const size_t sz = 10;
  peloton::task::TaskQueue tq(sz);
  peloton::task::WorkerPool wp(1, &tq);

  std::vector<void *> params1;
  std::string p1 = "monday";
  params1.push_back((void*)&p1);

  peloton::task::Task task1(func1, params1);
  tq.SubmitTask(shared_ptr<Task>(task1));
  std::cout << "master, finish" <<std::endl;
  wp.Shutdown();
}

} // end of test
} // end of peloton