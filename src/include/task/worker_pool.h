//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.h
//
// Identification: src/include/task/worker_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <memory>
#include "container/lock_free_queue.h"

//TODO: should be configurable
#define DEFAULT_NUM_WORKER_THREADS (std::thread::hardware_concurrency())
//TODO: make configurable, choose some more reasonable default value
#define DEFAULT_TASK_QUEUE_LENGTH (std::thread::hardware_concurrency()*4)

namespace peloton{
namespace task{

class WorkerPool;

//task to submit to the queue
class Task {
  friend class WorkerThread;
public:
  inline Task() {};
  //
  inline Task(void(*func_ptr)(void *), void *func_args) :
          func_ptr_(func_ptr), func_args_(func_args) {};

private:
  void(*func_ptr_)(void *);
  void *func_args_;
};

// Data structure wraping worker thread
class WorkerThread{
public:

  void StartThread(WorkerPool* worker_pool);

  // poll work queue, until exiting
  static void PollForWork(WorkerThread* current_thread, WorkerPool* current_pool);

  // wait for the current task to complete and shutdown the thread;
  void Shutdown();

private:
  volatile bool shutdown_thread_;

  std::thread worker_thread_;

};

// generic pool for executing arbitrary tasks asynchronously
class WorkerPool {

  friend class WorkerThread;
public:
  static WorkerPool& GetInstance();

  // submit a task for asynchronous execution.
  void SubmitTask(void(*func_ptr)(void *), void *func_args);

  void Shutdown();


  WorkerPool(size_t num_threads, size_t task_queue_size);
private:
  //TODO: use Bili's work stealing queues?
  peloton::LockFreeQueue<Task> task_queue_;

  std::vector<std::unique_ptr<WorkerThread>> worker_threads_;

};


}//task
}//peloton
