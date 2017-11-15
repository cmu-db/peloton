//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.cpp
//
// Identification: src/threadpool/worker_pool.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/worker_pool.h"

namespace peloton {
namespace threadpool {

WorkerPool::WorkerPool(size_t num_workers, TaskQueue *task_queue) :
  num_workers_(num_workers), task_queue_(task_queue) {}

void WorkerPool::Startup() {
  for (size_t i = 0; i < num_workers_; i++) {
    // start thread on construction
    std::unique_ptr<Worker> worker(new Worker());
    worker->Start(task_queue_);
    workers_.push_back(std::move(worker));
  }
}

void WorkerPool::Shutdown() {
  for (auto &worker: workers_) {
    worker->Stop();
  }
  workers_.clear();
}

}  // namespace threadpool
}  // namespace peloton
