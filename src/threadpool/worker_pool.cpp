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

WorkerPool::WorkerPool(size_t num_threads, TaskQueue *task_queue) {
  task_queue_ = task_queue;
  for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
    // start thread on construction
    std::unique_ptr<Worker> worker(new Worker());
    worker->Start(task_queue_);
    worker_threads_.push_back(std::move(worker));
  }
}

void WorkerPool::Shutdown() {
  for (auto &thread : worker_threads_) {
    thread->Stop();
  }
  worker_threads_.clear();
}

}  // namespace threadpool
}  // namespace peloton
