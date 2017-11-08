//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker.cpp
//
// Identification: src/threadpool/worker.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/logger.h"
#include "common/logger.h"
#define EMPTY_COUNT_BOUND 10
#define LOGGER_PAUSE_TIME 50

namespace peloton {
namespace threadpool {

void Logger::StartThread(LoggerPool* current_pool) {
  shutdown_thread_ = false;
  logger_thread_ = std::thread(Logger::PollForWork, this, current_pool);
}

void Logger::PollForWork(Logger* current_thread, LoggerPool* current_pool) {
  size_t empty_count = 0;
  std::shared_ptr<LogTask> t;
  while (!current_thread->shutdown_thread_ ||
         !current_pool->task_queue_->IsEmpty()) {
    // poll the queue
    if (!current_pool->task_queue_->PollTask(t)) {
      empty_count++;
      if (empty_count == EMPTY_COUNT_BOUND) {
        empty_count = 0;
        usleep(LOGGER_PAUSE_TIME);
      }
      continue;
    }
    LOG_TRACE("Grab one task, going to execute it");
    // call the threadpool
    t->ExecuteTask();
    LOG_TRACE("Finish one task");
    empty_count = 0;
  }
}

void Logger::Shutdown() {
  shutdown_thread_ = true;
  logger_thread_.join();
}

LoggerPool::LoggerPool(size_t num_threads, LogTaskQueue* task_queue) {
  this->task_queue_ = task_queue;
  for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
    // start thread on construction
    std::unique_ptr<Logger> logger = std::make_unique<Logger>();
    logger->StartThread(this);
    logger_threads_.push_back(std::move(logger));
  }
}

void LoggerPool::Shutdown() {
  for (auto& thread : logger_threads_) {
    thread->Shutdown();
  }
  logger_threads_.clear();
  return;
}

}  // namespace threadpool
}  // namespace peloton
