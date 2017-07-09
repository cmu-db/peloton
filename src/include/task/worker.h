//
// Created by tim on 09/07/17.
//

#pragma once

#include <vector>
#include <thread>
#include <memory>

#include "task/task.h"

namespace peloton{
namespace task{
class WorkerPool;
class Worker {
  friend class WorkerPool;
 public:
  void StartThread(WorkerPool* worker_pool);

  // poll work queue, until exiting
  static void PollForWork(Worker* current_thread, WorkerPool* current_pool);

  // wait for the current task to complete and shutdown the thread;
  void Shutdown();
 private:
  volatile bool shutdown_thread_;
  std::thread worker_thread_;
};

class WorkerPool {
  friend class Worker;
 public:
  //static WorkerPool& GetInstance();
  // submit a task for asynchronous execution.
  void Shutdown();
  WorkerPool(size_t num_threads, TaskQueue *taskQueue);
 private:
  std::vector<std::unique_ptr<Worker>> worker_threads_;
  TaskQueue *task_queue_;
};

}
}


