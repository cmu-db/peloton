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
#include "settings/settings_manager.h"

namespace peloton {
namespace logging {

// Method to enqueue the logging task
ResultType WalLogManager::LogTransaction(std::vector<LogRecord> log_records) {
//  LogTransactionArg* arg = new LogTransactionArg(log_records);
  auto on_complete = [this]() {
    //this->p_status_ = p_status;
    //result = std::move(values);
    task_callback_(task_callback_arg_);
  };
  threadpool::LoggerQueuePool::GetInstance().SubmitTask([log_records, on_complete] {
    WalLogManager::WriteTransaction(log_records, on_complete);
  });
  return ResultType::LOGGING;
}

// Actual method called by the logger thread
void WalLogManager::WriteTransaction(std::vector<LogRecord> log_records, std::function<void()> on_complete) {
  WalLogger* wl = new WalLogger(0, settings::SettingsManager::GetString(settings::SettingId::log_directory));
  wl->WriteTransaction(log_records);
  delete wl;
  on_complete();
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
  }
}
void WalLogManager::DoRecovery() {
  WalRecovery* wr = new WalRecovery(0, settings::SettingsManager::GetString(settings::SettingId::log_directory));
  wr->StartRecovery();
  delete wr;
}
}
}
