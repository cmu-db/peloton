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
#include <cassert>

#define NUM_THREAD 10

namespace peloton {

// global singleton
ThreadManager &ThreadManager::GetInstance(void) {
  static ThreadManager thread_manager(NUM_THREAD);
  return thread_manager;
}


//ThreadManager &ThreadManager::GetServerThreadPool(void) {
//  static ThreadManager server_thread_pool(NUM_THREAD);
//  return server_thread_pool;
//}
//
//ThreadManager &ThreadManager::GetClientThreadPool(void) {
//  static ThreadManager client_thread_pool(NUM_THREAD);
//  return client_thread_pool;
//}

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

//===--------------------------------------------------------------------===//
// Thread Pool implemented using pthread. The functionality is similar
// wtih thread manager. We probably combine this with thread manager
// in the future
//===--------------------------------------------------------------------===//

ThreadPool &ThreadPool::GetServerThreadPool(void) {
  static ThreadManager server_thread_pool(NUM_THREAD);
  return server_thread_pool;
}

ThreadPool &ThreadPool::GetClientThreadPool(void) {
  static ThreadManager client_thread_pool(NUM_THREAD);
  return client_thread_pool;
}

ThreadPool::ThreadPool(int threads) :
    terminate_(false) {

    pthread_attr_init(&attr_);
    pthread_mutex_init(&thread_pool_mutex_, NULL);
    pthread_cond_init(&condition_, NULL);

    // Create number of required threads and add them to the thread pool vector.
    for(int i = 0; i < threads; i++) {
        pthread_t threadx;
        pthread_create(&threadx, &attr_, &ThreadPool::InvokeEntry, static_cast<void *>(this));
        thread_pool_.emplace_back(threadx);
    }
}

ThreadPool::~ThreadPool() {

    // Put lock on task mutex.
    pthread_mutex_lock(&thread_pool_mutex_);

    if (!terminate_) {
        // Set termination flag to true.
        terminate_ = true;
    }

    pthread_mutex_unlock(&thread_pool_mutex_);

    // Wake up all threads.
    pthread_cond_broadcast(&condition_);

    // Join all threads.
    for (pthread_t &thread : thread_pool_) {
        pthread_join(thread, NULL);
    }

}

void ThreadPool::AddTask(std::function<void()> f) {

    // Put unique lock on task mutex.
    pthread_mutex_lock(&thread_pool_mutex_);

    // Push task into queue.
    task_pool_.push(f);

    // unlock
    pthread_mutex_unlock(&thread_pool_mutex_);

    // Wake up one thread.
    pthread_cond_signal(&condition_);

}

/*
 * @Param args include:
 *                      ThreadPool instance
 *                      Message to be processed
 */
void ThreadPool::InvokeEntry(void* self) {
    ((ThreadPool *)self)->Invoke();
}

void ThreadPool::Invoke() {

    std::function<void()> task;

    while(true) {

        // Put the lock on task mutex.
        pthread_mutex_lock(&thread_pool_mutex_);

        if (task_pool_.empty() && terminate_ == false) {
            pthread_cond_wait(&condition_, &thread_pool_mutex_);
        }

        // If termination signal received and queue is empty then exit else continue clearing the queue.
        if (terminate_ && task_pool_.empty()) {
            return;
        }

        // if running here, task_pool must not be empty
        assert(!task_pool_.empty());

        // Get next task in the queue.
        task = task_pool_.front();

        // Remove it from the queue.
        task_pool_.pop();

        pthread_mutex_unlock(&mutex);

        // Execute the task.
        task();

    } // end while
}

}  // End peloton namespace
