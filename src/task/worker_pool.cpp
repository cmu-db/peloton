//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.cpp
//
// Identification: src/task/worker_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/task/worker_pool.h"
#include <unistd.h>

namespace peloton{
namespace task{

void WorkerThread::StartThread(WorkerPool* current_pool){
  shutdown_thread_ = false;
  worker_thread_ = std::thread(WorkerThread::PollForWork, this, current_pool);
}

void WorkerThread::PollForWork(WorkerThread* current_thread, WorkerPool* current_pool){
  size_t empty_count = 0;
  Task t;
  while(!current_thread->shutdown_thread_ || !current_pool->task_queue_.IsEmpty()){
    // poll the queue
    if(!current_pool->task_queue_.Dequeue(t)){
      empty_count++;
      if (empty_count == 10){
        empty_count = 0;
        usleep(1);
      }
      continue;
    }
    // call the task
    t.func_ptr_(t.func_args_);
    empty_count = 0;
  }
}

void WorkerThread::Shutdown() {
  shutdown_thread_ = true;
  worker_thread_.join();
}

WorkerPool::WorkerPool(size_t num_threads, size_t task_queue_size) :
        task_queue_(task_queue_size){
  for (size_t thread_id = 0; thread_id < num_threads; thread_id++){
    // start thread on construction
    std::unique_ptr<WorkerThread> worker(new WorkerThread());
    worker->StartThread(this);
    worker_threads_.push_back(std::move(worker));
  }
}

WorkerPool& WorkerPool::GetInstance() {
  static WorkerPool wp(DEFAULT_NUM_WORKER_THREADS, DEFAULT_TASK_QUEUE_LENGTH);
  return wp;
}

void WorkerPool::SubmitTask(void(*func_ptr)(void *), void *func_args) {
  task_queue_.Enqueue(Task(func_ptr, func_args));
}

void WorkerPool::Shutdown() {
  for (auto& thread : worker_threads_){
    thread->Shutdown();
  }
  worker_threads_.clear();
  return;
}


}//task
}//peloton
