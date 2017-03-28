//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.h
//
// Identification: src/include/logging/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_manager.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// log Manager
//===--------------------------------------------------------------------===//

class LogicalLogManager : public LogManager {
 public:
  LogicalLogManager(const LogicalLogManager &) = delete;
  LogicalLogManager &operator=(const LogicalLogManager &) = delete;
  LogicalLogManager(LogicalLogManager &&) = delete;
  LogicalLogManager &operator=(LogicalLogManager &&) = delete;

  LogicalLogManager(int thread_count) : logger_thread_count_(thread_count) {}

  virtual ~LogicalLogManager() {}

  static LogicalLogManager &GetInstance(const int thread_count = 1) {
    static LogicalLogManager log_manager(thread_count);
    return log_manager;
  }

  virtual void Reset() { is_running_ = false; }

  virtual void StartLogging(std::vector<std::unique_ptr<std::thread>> & UNUSED_ATTRIBUTE) {}

  virtual void StartLogging() {}

  virtual void StopLogging() {}

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  virtual void LogBegin() {}

  virtual void LogInsert(const ItemPointer & UNUSED_ATTRIBUTE) {}
  
  virtual void LogUpdate(const ItemPointer & UNUSED_ATTRIBUTE) {}
  
  virtual void LogDelete(const ItemPointer & UNUSED_ATTRIBUTE) {}

 private:
  int logger_thread_count_;

};

}  // namespace gc
}  // namespace peloton
