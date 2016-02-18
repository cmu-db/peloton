//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// thread_manager.h
//
// Identification: src/backend/common/thread_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <set>
#include <mutex>
#include <memory>
#include <thread>

namespace peloton {

//===--------------------------------------------------------------------===//
// Thread Manager
//===--------------------------------------------------------------------===//

class ThreadManager {
  ThreadManager & operator=(const ThreadManager &) = delete;
  ThreadManager & operator=(ThreadManager &&) = delete;

 public:
  // global singleton
  static ThreadManager &GetInstance(void);

  bool AttachThread(std::shared_ptr<std::thread> thread);

  bool DetachThread(std::shared_ptr<std::thread> thread);

 private:

  // thread pool
  std::set<std::shared_ptr<std::thread>> thread_pool;

  // thread pool mutex
  std::mutex thread_pool_mutex;
};

}  // End peloton namespace
