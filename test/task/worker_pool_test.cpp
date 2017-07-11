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
#include <time.h>
namespace peloton {
namespace test {

class WorkerPoolTests : public PelotonTest {};

void shortTask(void* param) {
  int num = *((int *)param);
  std::thread::id id = std::this_thread::get_id();
  std::cout << "----- thread " << id << " starts, short " << num << std::endl;
  usleep(num * 100000);
  std::cout << "thread " << id << " finishes, short " << num << std::endl;
}

void longTask(void* param) {
  int num = *((int*)param);
  std::thread::id id = std::this_thread::get_id();
  std::cout << "----- thread " << id << " starts, long " << num << std::endl;
  usleep(num * 10000000);
  std::cout << "thread " << id << " finishes, long " << num << std::endl;
}

void masterCallback() {
  LOG_INFO("master activate");
}

TEST_F(WorkerPoolTests, MultiWorkerTest) {
  LOG_INFO("master starts");
  const size_t sz = 20;
  int task_num = 4, i = 0;
  task::TaskQueue tq(sz);
  task::WorkerPool wp(4, &tq);

  std::vector<std::shared_ptr<task::Task>> task_v;
  std::vector<int> params;
  for (i = 0; i < task_num; i++) {
    params.push_back(1);
  }
  clock_t t1 = clock();
  for(i = 0; i < task_num; i++) {
    if (i % 2 == 0) {
      task_v.push_back(std::make_shared<task::Task>(longTask, &params.at(i)));
    } else {
      task_v.push_back(std::make_shared<task::Task>(shortTask, &params.at(i)));
    }
  }
  tq.SubmitTaskBatch(task_v);
  wp.Shutdown();
  clock_t t2 = clock();
  std::cout << "multithread: " << t2 - t1 << std::endl;
  for(i = 0; i < task_num; i++) {
    if (i % 2 == 0) {
      longTask(&params.at(0));
    } else {
      shortTask(&params.at(0));
    }
  }
  std::cout << "sequential: " << clock() - t2 << std::endl;
  LOG_INFO("master finishes");

}

} // end of test
} // end of peloton