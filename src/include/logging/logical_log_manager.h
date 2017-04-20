//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_log_manager.h
//
// Identification: src/include/logging/logical_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_manager.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// logical log Manager
//===--------------------------------------------------------------------===//


/**
 * logging file name layout :
 * 
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_id | database_id | table_id | operation_type | data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for logical logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class LogicalLogManager : public LogManager {
 public:
  LogicalLogManager(const LogicalLogManager &) = delete;
  LogicalLogManager &operator=(const LogicalLogManager &) = delete;
  LogicalLogManager(LogicalLogManager &&) = delete;
  LogicalLogManager &operator=(LogicalLogManager &&) = delete;

  LogicalLogManager(const int thread_count) : logger_thread_count_(thread_count) {}

  virtual ~LogicalLogManager() {}

  static LogicalLogManager &GetInstance(const int thread_count = 1) {
    static LogicalLogManager log_manager(thread_count);
    return log_manager;
  }

  // virtual void SetDirectories(const std::vector<std::string> &logging_dirs) override {
  //   logger_dirs_ = logging_dirs;

  //   if (logging_dirs.size() > 0) {
  //     pepoch_dir_ = logging_dirs.at(0);
  //   }
  //   // check the existence of logging directories.
  //   // if not exists, then create the directory.
  //   for (auto logging_dir : logging_dirs) {
  //     if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
  //       LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
  //       bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
  //       if (res == false) {
  //         LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
  //       }
  //     }
  //   }

  //   logger_count_ = logging_dirs.size();
  //   for (size_t i = 0; i < logger_count_; ++i) {
  //     loggers_.emplace_back(new PhyLogLogger(i, logging_dirs.at(i)));
  //   }
  // }

  // virtual const std::vector<std::string> &GetDirectories() override {
  //   return logger_dirs_;
  // }


  virtual void StartLogging(std::vector<std::unique_ptr<std::thread>> & UNUSED_ATTRIBUTE) override {}

  virtual void StartLogging() override {}

  virtual void StopLogging() override {}

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) override {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) override {}

  virtual size_t GetTableCount() override { return 0; }

  virtual void LogBegin() override {}

  virtual void LogEnd() override {}

  virtual void LogInsert(const ItemPointer & UNUSED_ATTRIBUTE) override {}
  
  virtual void LogUpdate(const ItemPointer & UNUSED_ATTRIBUTE) override {}
  
  virtual void LogDelete(const ItemPointer & UNUSED_ATTRIBUTE) override { }

 private:
  int logger_thread_count_;

  // std::atomic<oid_t> worker_count_;

  // std::vector<std::string> logger_dirs_;

  // std::vector<std::shared_ptr<PhyLogLogger>> loggers_;

  // std::unique_ptr<std::thread> pepoch_thread_;
  // volatile bool is_running_;

  // std::string pepoch_dir_;

  // const std::string pepoch_filename_ = "pepoch";

};

}  // namespace logging
}  // namespace peloton
