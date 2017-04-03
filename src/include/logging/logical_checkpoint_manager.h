//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_checkpoint_manager.h
//
// Identification: src/include/logging/logical_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/checkpoint_manager.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// logical checkpoint Manager
//===--------------------------------------------------------------------===//

class LogicalCheckpointManager : public CheckpointManager {
 public:
  LogicalCheckpointManager(const LogicalCheckpointManager &) = delete;
  LogicalCheckpointManager &operator=(const LogicalCheckpointManager &) = delete;
  LogicalCheckpointManager(LogicalCheckpointManager &&) = delete;
  LogicalCheckpointManager &operator=(LogicalCheckpointManager &&) = delete;

  LogicalCheckpointManager(const int thread_count) : checkpointer_thread_count_(thread_count) {}

  virtual ~LogicalCheckpointManager() {}

  static LogicalCheckpointManager &GetInstance(const int thread_count = 1) {
    static LogicalCheckpointManager checkpoint_manager(thread_count);
    return checkpoint_manager;
  }

  virtual void Reset() { is_running_ = false; }

  virtual void StartLogging(std::vector<std::unique_ptr<std::thread>> & UNUSED_ATTRIBUTE) {}

  virtual void StartLogging() {}

  virtual void StopLogging() {}

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

 private:
  int checkpointer_thread_count_;

};

}  // namespace logging
}  // namespace peloton
