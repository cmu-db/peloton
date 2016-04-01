//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_manager.h
//
// Identification: src/backend/common/thread_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/mutex.h"

#include <pthread.h>

#include <atomic>
#include <vector>
#include <mutex>
#include <memory>
#include <thread>
#include <condition_variable>
#include <functional>
#include <queue>

namespace peloton {

//===--------------------------------------------------------------------===//
// Thread Manager
//===--------------------------------------------------------------------===//

class ThreadManager {
  ThreadManager &operator=(const ThreadManager &) = delete;
  ThreadManager &operator=(ThreadManager &&) = delete;

 public:
  // global singleton
  static ThreadManager &GetInstance(void);

  // The main function: add task into the task queue
  void AddTask(std::function<void()> f);

  // The number of the threads should be inited
  ThreadManager(int threads);
  ~ThreadManager();

 private:
  // thread pool
  std::vector<std::thread> thread_pool_;

  // Queue to keep track of incoming tasks.
  std::queue<std::function<void()>> task_pool_;

  // thread pool mutex
  std::mutex thread_pool_mutex_;

  // Condition variable.
  std::condition_variable condition_;

  // Indicates that pool needs to be shut down and terminated.
  bool terminate_;

  // Function that will be invoked by our threads.
  void Invoke();
};

//===--------------------------------------------------------------------===//
// Thread Pool implemented using pthread. The functionality is similar
// wtih thread manager. We probably combine this with thread manager
// in the future
//===--------------------------------------------------------------------===//

class ThreadPool {
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

 public:
  // global singleton
  // TODO: For now, rpc client and server use separated thread pool
  //       global thread pool can lead to starving for client/server
  static ThreadPool &GetInstance(void);

  // The main function: add task into the task queue
  void AddTask(std::function<void()> f);

  // TODO: we don't need this API? by Michael
  // bool AttachThread(std::shared_ptr<std::thread> thread);

  // TODO: we don't need this API? by Michael
  // bool DetachThread(std::shared_ptr<std::thread> thread);

  // The number of the threads should be inited
  ThreadPool(int threads);
  ~ThreadPool();

 protected:
  // Function that will be invoked by our threads.
  void Invoke();

 private:
  // pthread can only access static function
  static void *InvokeEntry(void *args);

  // thread pool
  std::vector<pthread_t> thread_pool_;

  // Queue to keep track of incoming tasks.
  std::queue<std::function<void()>> task_pool_;

  // thread pool mutex
  Mutex mutex_;

  // thread cond_
  Condition cond_;

  // Indicates that pool needs to be shut down and terminated.
  bool terminate_;
};

}  // End peloton namespace
