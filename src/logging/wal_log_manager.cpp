//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_manager.cpp
//
// Identification: src/logging/wal_log_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "logging/wal_log_manager.h"
#include "logging/logging_util.h"
#include "threadpool/logger_queue_pool.h"
#include "logging/wal_recovery.h"
#include "logging/wal_logger.h"

namespace peloton {
namespace logging {

// Method to enqueue the logging task
ResultType WalLogManager::LogTransaction(std::vector<LogRecord> log_records) {
  LogTransactionArg* arg = new LogTransactionArg(log_records);
  threadpool::LoggerQueuePool::GetInstance().SubmitTask(
      WalLogManager::WriteTransactionWrapper, arg, task_callback_,
      task_callback_arg_);
  LOG_DEBUG("Submit Task into LogQueuePool");
  return ResultType::QUEUING;
}

// Static method that accepts void pointer
void WalLogManager::WriteTransactionWrapper(void* arg_ptr) {
  LogTransactionArg* arg = (LogTransactionArg*)arg_ptr;
  WriteTransaction(arg->log_records_);
  delete (arg);
}

// Actual method called by the logger thread
void WalLogManager::WriteTransaction(std::vector<LogRecord> log_records) {
  WalLogger* wl = new WalLogger(0, "/tmp/log");
  wl->WriteTransaction(log_records);
  delete wl;
}

void WalLogManager::SetDirectory(std::string logging_dir) {
  // check the existence of logging directories.
  // if not exists, then create the directory.
  if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
    LOG_INFO("Logging directory %s is not accessible or does not exist",
             logging_dir.c_str());
    bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
    if (res == false) {
      LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
    }
    log_directory_ = logging_dir;
  }
}
void WalLogManager::DoRecovery() {
  WalRecovery* wr = new WalRecovery(0, "/tmp/log");
  wr->StartRecovery();
  delete wr;
}
}
}
