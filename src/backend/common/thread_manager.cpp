//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_manager.cpp
//
// Identification: src/backend/common/thread_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "backend/common/thread_manager.h"

#define NUM_THREAD 10

namespace peloton {

// global singleton
ThreadManager &ThreadManager::GetInstance(void) {
  static ThreadManager thread_manager(NUM_THREAD);
  return thread_manager;
}

ThreadManager::ThreadManager(int threads) : terminate_(false) {
  // Create number of required threads and add them to the thread pool vector.
  for (int thread_itr = 0; thread_itr < threads; thread_itr++) {
    thread_pool_.emplace_back(std::thread(&ThreadManager::Invoke, this));
  }
}

ThreadManager::~ThreadManager() {
  if (!terminate_) {
    // Scope based locking.
    {
      // Put unique lock on task mutex.
      std::unique_lock<std::mutex> lock(thread_pool_mutex_);

      // Set termination flag to true.
      terminate_ = true;
    }

    // Wake up all threads.
    condition_.notify_all();

    // Join all threads.
    for (std::thread &thread : thread_pool_) {
      thread.join();
    }
  }  // end if
}

void ThreadManager::AddTask(std::function<void()> f) {
  // Scope based locking.
  {
    // Put unique lock on task mutex.
    std::unique_lock<std::mutex> lock(thread_pool_mutex_);

    // Push task into queue.
    task_pool_.push(f);
  }

  // Wake up one thread.
  condition_.notify_one();
}

void ThreadManager::Invoke() {
  std::function<void()> task;

  while (true) {
    // Scope based locking.
    {
      // Put unique lock on task mutex.
      std::unique_lock<std::mutex> lock(thread_pool_mutex_);

      // Wait until queue is not empty or termination signal is sent.
      condition_.wait(lock,
                      [this] { return !task_pool_.empty() || terminate_; });

      // If termination signal received and queue is empty then exit else
      // continue clearing the queue.
      if (terminate_ && task_pool_.empty()) {
        return;
      }

      // Get next task in the queue.
      task = task_pool_.front();

      // Remove it from the queue.
      task_pool_.pop();
    }
    // end scope

    // Execute the task.
    task();

  }  // end while
}

//===--------------------------------------------------------------------===//
// Thread Pool implemented using pthread. The functionality is similar
// wtih thread manager, but mutex and cond are wrappered. We probably
// combine this with thread manager in the future
//===--------------------------------------------------------------------===//

// global singleton
ThreadPool &ThreadPool::GetInstance(void) {
  static ThreadPool thread_pool(NUM_THREAD);
  return thread_pool;
}

ThreadPool::ThreadPool(int threads) : cond_(&mutex_), terminate_(false) {
  // Create number of required threads and add them to the thread pool vector.
  for (int i = 0; i < threads; i++) {
    pthread_t threadx;
    pthread_create(&threadx, NULL, ThreadPool::InvokeEntry,
                   static_cast<void *>(this));
    thread_pool_.emplace_back(threadx);
  }
}

ThreadPool::~ThreadPool() {
  // Put lock on task mutex.
  mutex_.Lock();

  if (!terminate_) {
    // Set termination flag to true.
    terminate_ = true;
  }

  mutex_.UnLock();

  // Wake up all threads.
  cond_.Broadcast();

  // Join all threads.
  //    for (pthread_t &thread : thread_pool_) {
  //        pthread_join(thread, NULL);
  //    }
}

void ThreadPool::AddTask(std::function<void()> f) {
  // Put unique lock on task mutex.
  mutex_.Lock();

  // Push task into queue.
  task_pool_.push(f);

  // unlock
  mutex_.UnLock();

  // Wake up one thread.
  cond_.Signal();
}

/*
 * @Param args
 *                      ThreadPool instance
 */
void *ThreadPool::InvokeEntry(void *self) {
  ((ThreadPool *)self)->Invoke();

  pthread_exit((void *)0);
  return NULL;
}

void ThreadPool::Invoke() {
  std::function<void()> task;

  while (true) {
    // Put the lock on task mutex.
    mutex_.Lock();

    // wait will "atomically" unlock the mutex,
    // allowing others access to the condition variable (for signalling)
    while (task_pool_.empty() && terminate_ == false) {
      cond_.Wait();
    }

    // If termination signal received and queue is empty then exit else continue
    // clearing the queue.
    if (terminate_ && task_pool_.empty()) {
      return;
    }

    // if running here, task_pool must not be empty
    assert(!task_pool_.empty());

    // Get next task in the queue.
    task = task_pool_.front();

    // Remove it from the queue.
    task_pool_.pop();

    mutex_.UnLock();

    // Execute the task.
    task();

  }  // end while
}

}  // End peloton namespace
