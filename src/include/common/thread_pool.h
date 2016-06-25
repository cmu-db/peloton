//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool.h
//
// Identification: src/include/common/thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

namespace peloton {

class ThreadPool {

 public:

  ThreadPool();

  ThreadPool(size_t num_threads);

  // Enqueue Task
  template<class F, class... Args>
  auto Enqueue(F&& f, Args&&... args) ->
  std::future<typename std::result_of<F(Args...)>::type>;

  ~ThreadPool();

  size_t GetNumThreads() const {
    return num_threads;
  }

 private:

  // need to keep track of threads so we can join them
  std::vector< std::thread > workers;

  // the task queue
  std::queue< std::function<void()> > tasks;

  // synchronization
  std::mutex queue_mutex;

  std::condition_variable condition;

  bool stop;

  // number of threads
  size_t num_threads;

};


}  // End peloton namespace
