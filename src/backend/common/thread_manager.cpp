//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// thread_manager.cpp
//
// Identification: src/backend/common/thread_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/thread_manager.h"

<<<<<<< HEAD
#define NUM_THREAD 10

namespace peloton {

// global singleton
/*
ThreadManager &ThreadManager::GetInstance(void) {
  static ThreadManager thread_manager(NUM_THREAD);
  return thread_manager;
}
*/

ThreadManager &ThreadManager::GetServerThreadPool(void) {
  static ThreadManager server_thread_pool(NUM_THREAD);
  return server_thread_pool;
}

ThreadManager &ThreadManager::GetClientThreadPool(void) {
  static ThreadManager client_thread_pool(NUM_THREAD);
  return client_thread_pool;
}

ThreadManager::ThreadManager(int threads) :
    terminate_(false) {

    // Create number of required threads and add them to the thread pool vector.
    for(int i = 0; i < threads; i++) {
        thread_pool_.emplace_back(std::thread(&ThreadManager::Invoke, this));
    }
}

ThreadManager::~ThreadManager() {

    if (!terminate_) {
        // Scope based locking.
        {
            // Put unique lock on task mutex.
            std::unique_lock < std::mutex > lock(thread_pool_mutex_);

            // Set termination flag to true.
            terminate_ = true;
        }

        // Wake up all threads.
        condition_.notify_all();

        // Join all threads.
        for (std::thread &thread : thread_pool_) {
            thread.join();
        }
    } // end if
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

    while(true) {
        // Scope based locking.
        {
            // Put unique lock on task mutex.
            std::unique_lock<std::mutex> lock(thread_pool_mutex_);

            // Wait until queue is not empty or termination signal is sent.
            condition_.wait(lock, [this]{ return !task_pool_.empty() || terminate_; });

            // If termination signal received and queue is empty then exit else continue clearing the queue.
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

    } // end while
}
=======
namespace peloton {

// global singleton
ThreadManager &ThreadManager::GetInstance(void) {
  static ThreadManager thread_manager;
  return thread_manager;
}

bool ThreadManager::AttachThread(std::shared_ptr<std::thread> thread) {
  {
    std::lock_guard<std::mutex> thread_pool_lock(thread_pool_mutex);

    // Check if thread exists in thread pool
    const bool exists = (thread_pool.find(thread) != thread_pool.end());
    if(exists == false){
      thread_pool.insert(thread);
      return true;
    }

    return false;
  }
}

bool ThreadManager::DetachThread(std::shared_ptr<std::thread> thread) {
  {
    std::lock_guard<std::mutex> thread_pool_lock(thread_pool_mutex);

    // Check if thread exists in thread pool
    const bool exists = (thread_pool.find(thread) != thread_pool.end());
    if(exists == true){
      thread_pool.erase(thread);
      return true;
    }

    return false;
  }
}

>>>>>>> refs/remotes/upstream/master

}  // End peloton namespace
