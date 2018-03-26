//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager_factory.h
//
// Identification: src/include/logging/checkpoint_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/checkpoint_manager.h"
#include "logging/logical_checkpoint_manager.h"
#include "logging/timestamp_checkpoint_manager.h"

namespace peloton {
namespace logging {

class CheckpointManagerFactory {
 public:

  static CheckpointManager& GetInstance() {
    switch (checkpointing_type_) {

      case CheckpointingType::LOGICAL:
        return LogicalCheckpointManager::GetInstance(checkpointing_thread_count_);
      
      case CheckpointingType::TIMESTAMP:
        return TimestampCheckpointManager::GetInstance(checkpointing_thread_count_);

      default:
        return CheckpointManager::GetInstance();
    }
  }

  static void Configure(const int thread_count = 1, const CheckpointingType type = CheckpointingType::TIMESTAMP) {
    if (thread_count == 0) {
      checkpointing_type_ = CheckpointingType::OFF;
    } else {
      checkpointing_type_ = type;
      checkpointing_thread_count_ = thread_count;
    }
  }

  inline static CheckpointingType GetCheckpointingType() { return checkpointing_type_; }

  inline static int GetCheckpointingThreadCount() { return checkpointing_thread_count_; }

private:
  // checkpointing type
  static CheckpointingType checkpointing_type_;

  static int checkpointing_thread_count_;
};

} // namespace logging
} // namespace peloton
