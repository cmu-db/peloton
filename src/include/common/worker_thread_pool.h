//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_thread_pool.h
//
// Identification: src/include/common/worker_thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

namespace peloton {
// a wrapper for boost worker thread pool.
class WorkerThreadPool {
 public:
  WorkerThreadPool() : pool_size_(0), work_(io_service_) {}
  WorkerThreadPool(const size_t &pool_size)
      : pool_size_(pool_size), work_(io_service_) {}

  ~WorkerThreadPool() {
    io_service_.stop();
    thread_pool_.join_all();
  }

  void InstantiatePool(const size_t &pool_size) {
    pool_size_ = pool_size;
    ALWAYS_ASSERT(pool_size_ != 0);
    for (size_t i = 0; i < pool_size_; ++i) {
      // add thread to thread pool.
      thread_pool_.create_thread(
          boost::bind(&boost::asio::io_service::run, &io_service_));
    }
  }

  // submit task to thread pool.
  // it accepts a function and a set of function parameters as parameters.
  template <typename FunctionType, typename... ParamTypes>
  void SubmitTask(FunctionType &&func, const ParamTypes &&... params) {
    // add task to thread pool.
    io_service_.post(std::bind(func, params...));
  }

 private:
  WorkerThreadPool(const WorkerThreadPool &);
  WorkerThreadPool &operator=(const WorkerThreadPool &);

 private:
  // number of threads in the thread pool.
  size_t pool_size_;
  // real thread pool that holds a set of threads.
  boost::thread_group thread_pool_;
  // io_service provides IO functionality of asynchronize services.
  boost::asio::io_service io_service_;
  // io_service::work is responsible for starting the io_service processing
  // loop.
  boost::asio::io_service::work work_;
};

}  // End peloton namespace
