//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_queue_pool.h
//
// Identification: test/threadpool/Logger_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <atomic>
#include "worker_pool.h"
#include "logging/log_buffer.h"
#include "logging/wal_logger.h"


// TODO: tune these variables
constexpr static size_t kDefaultLoggerQueueSize = 32;
constexpr static size_t kDefaultLoggerPoolSize = 1;
constexpr static size_t kDefaultNumTokens = 8;


namespace peloton {
namespace threadpool {

using LoggerQueue = peloton::LockFreeQueue<logging::LogBuffer *>;

void LoggerFunc(std::atomic_bool *is_running, LoggerQueue *logger_queue);


/**
* @class LoggerQueuePool
* @brief Wrapper class for single queue and single pool
* One should use this if possible.
*/
class LoggerQueuePool {
  public:
    LoggerQueuePool(size_t num_workers)
            : logger_queue_(kDefaultLoggerQueueSize),
              num_workers_(num_workers),
              is_running_(false) {
      PELOTON_ASSERT(num_workers == 1);
      logger_queue_.GenerateTokens(log_tokens_, kDefaultNumTokens);
    }

    ~LoggerQueuePool() {
      if(is_running_)
        Shutdown();
    }

    inline int GetLogToken(){
      return next_token_++;
    }


    void Startup() {
      is_running_ = true;

      for (size_t i = 0; i < num_workers_; i++) {
        loggers_.emplace_back(LoggerFunc, &is_running_, &logger_queue_);
      }
    }

    void Shutdown() {
      is_running_ = false;

      for (auto &logger : loggers_) {
        logger.join();
      }
      loggers_.clear();
    }

    void SubmitLogBuffer(logging::LogBuffer *buffer) {
      if (is_running_ == false)
        PELOTON_ASSERT(false);

      logger_queue_.Enqueue(std::move(buffer));
    }

    void SubmitLogBuffer(int token, logging::LogBuffer *buffer) {
      if (is_running_ == false)
        PELOTON_ASSERT(false);

      int idx = (token)%kDefaultNumTokens;
      logger_queue_.Enqueue(*(log_tokens_[idx]), std::move(buffer));
    }

    static LoggerQueuePool &GetInstance() {
      static LoggerQueuePool logger_queue_pool(kDefaultLoggerPoolSize);
      return logger_queue_pool;
    }

  private:
    LoggerQueue logger_queue_;
    size_t num_workers_;

    std::vector<std::thread> loggers_;
    std::vector<logging::LogToken *> log_tokens_;
    std::atomic_bool is_running_;
    std::atomic<int> next_token_;


};

} // namespace threadpool
} // namespace peloton
