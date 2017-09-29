//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_log_manager.cpp
//
// Identification: src/backend/logging/reordered_phylog_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "logging/wal_log_manager.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"
#include "threadpool/logger_queue_pool.h"

namespace peloton {
namespace logging {

ResultType LogTransaction(std::vector<LogRecord> log_records){
    ResultType status = ResultType::SUCCESS;
    LogTransactionArg* arg = new LogTransactionArg(log_records, &status);
    threadpool::LoggerQueuePool::GetInstance().SubmitTask(WalLogManager::WriteTransactionWrapper, arg, task_callback_, task_callback_arg_);
    LOG_TRACE("Submit Task into MonoQueuePool");
    return status;
}

void SetDirectories(std::string logging_dir)
{
    // check the existence of logging directories.
    // if not exists, then create the directory.
      if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
        }
      }
  }
void WalLogManager::DoRecovery(){
    logger_->StartRecovery();
    logger_->WaitForRecovery();
 }

/*void WalLogManager::StartLoggers() {
//  logger_->StartLogging();
  is_running_ = true;
}

void WalLogManager::StopLoggers() {
//  logger_->StopLogging();
  is_running_ = false;

}*/

}
}
