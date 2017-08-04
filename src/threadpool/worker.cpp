//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.cpp
//
// Identification: src/threadpool/worker_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/worker.h"

#if defined(USE_mm_pause)
#define PAUSE _mm_pause
#else
#define PAUSE usleep
#endif

#define EMPTY_COUNT_BOUND 10
#define WORKER_PAUSE_TIME 10
namespace peloton {
namespace threadpool {

void Worker::StartThread(WorkerPool* current_pool){
  shutdown_thread_ = false;
  worker_thread_ = std::thread(Worker::PollForWork, this, current_pool);
}

void Worker::PollForWork(Worker* current_thread, WorkerPool* current_pool){
  size_t empty_count = 0;
  std::shared_ptr<Task> t;
  while(!current_thread->shutdown_thread_ || !current_pool->task_queue_->IsEmpty()){
    // poll the queue
    if(!current_pool->task_queue_->PollTask(t)){
      empty_count++;
      if (empty_count == EMPTY_COUNT_BOUND) {
        empty_count = 0;
        PAUSE(WORKER_PAUSE_TIME);
      }
      continue;
    }
    // call the threadpool
    t->ExecuteTask();
    empty_count = 0;
  }
}

void Worker::Shutdown() {
  shutdown_thread_ = true;
  worker_thread_.join();
}



WorkerPool::WorkerPool(size_t num_threads, TaskQueue *task_queue) {
  this->task_queue_ = task_queue;
  for (size_t thread_id = 0; thread_id < num_threads; thread_id++){
    // start thread on construction
    std::unique_ptr<Worker> worker = std::make_unique<Worker>();
    worker->StartThread(this);
    worker_threads_.push_back(std::move(worker));
  }
}
/*
WorkerPool& WorkerPool::GetInstance() {
  static WorkerPool wp(DEFAULT_NUM_WORKER_THREADS, DEFAULT_TASK_QUEUE_LENGTH);
  return wp;
}*/

void WorkerPool::Shutdown() {
  for (auto& thread : worker_threads_){
    thread->Shutdown();
  }
  worker_threads_.clear();
  return;
}

}//threadpool
}//peloton
