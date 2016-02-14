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


}  // End peloton namespace
