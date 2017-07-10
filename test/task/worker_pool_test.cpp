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
#include <unistd.h>

namespace peloton {
namespace test {

class WorkerPoolTests : public PelotonTest {};

void printString(std::vector<void *> args) {
  std::string *sp = static_cast<std::string *>(*args.begin());
  std::string str = *sp;
  std::thread::id id = std::this_thread::get_id();
  std::cout << "thread " << id << " get " << str.c_str() << std::endl;
}

class TaskGenerator {
 public:
  static std::shared_ptr<task::Task> getTask() {
    std::vector<void *> params;
    int len = 10;
    char str[len + 1];
    int i = 0;
    for (i = 0; i < len; i++) {
      str[i] = 'a' + rand() % 26;
    }
    str[10] = '\0';
    params.push_back((void*)&(str));
    return std::make_shared<task::Task>(printString, params);
  }
};

void masterCallback() {
  LOG_INFO("master activate");
}

TEST_F(WorkerPoolTests, MultiWorkerTest) {
  LOG_INFO("master starts");
  const size_t sz = 10;
  int task_num = 20, i = 0;
  task::TaskQueue tq(sz);
  task::WorkerPool wp(3, &tq);

  std::vector<std::shared_ptr<task::Task>> task_v;
  for(i = 0; i < task_num; i++) {
    task_v.push_back(TaskGenerator::getTask());
  }
  tq.SubmitTaskBatch(task_v);

  LOG_INFO("master finishes");
  wp.Shutdown();

}

} // end of test
} // end of peloton