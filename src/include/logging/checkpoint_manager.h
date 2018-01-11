//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.h
//
// Identification: src/include/logging/checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <thread>

#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/internal_types.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// checkpoint Manager
//===--------------------------------------------------------------------===//

class CheckpointManager {
 public:
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(CheckpointManager &&) = delete;

  CheckpointManager() : is_running_(false) {}

  virtual ~CheckpointManager() {}

  static CheckpointManager &GetInstance() {
    static CheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }

  virtual void Reset() { is_running_ = false; }

  // Get status of whether logging threads are running or not
  bool GetStatus() { return this->is_running_; }

  virtual void StartCheckpointing(std::vector<std::unique_ptr<std::thread>> & UNUSED_ATTRIBUTE) {}

  virtual void StartCheckpointing() {}

  virtual void StopCheckpointing() {}

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

 protected:
  volatile bool is_running_;
};

}  // namespace logging
}  // namespace peloton
