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

  LogicalLogManager() {}

  virtual ~LogicalLogManager() {}

  static LogicalLogManager &GetInstance() {
    static LogicalLogManager log_manager;
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

};

}  // namespace gc
}  // namespace peloton
