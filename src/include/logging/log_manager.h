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

#include <memory>
#include <vector>
#include <thread>

#include "common/logger.h"
#include "common/macros.h"
#include "type/types.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// log Manager
//===--------------------------------------------------------------------===//

class LogManager {
 public:
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

  LogManager() : is_running_(false) {}

  virtual ~LogManager() {}

  static LogManager &GetInstance() {
    static LogManager log_manager;
    return log_manager;
  }

  virtual void Reset() { is_running_ = false; }

  // Get status of whether logging thread is running or not
  bool GetStatus() { return this->is_running_; }

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

 protected:
  volatile bool is_running_;
};

}  // namespace gc
}  // namespace peloton
