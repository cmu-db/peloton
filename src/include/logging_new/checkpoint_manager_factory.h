//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager_factory.h
//
// Identification: src/include/concurrency/checkpoint_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


namespace peloton {
namespace logging {

class CheckpointManagerFactory {
 public:

  static CheckpointManager& GetInstance() {
    switch (checkpointing_type_) {

      case CheckpointingType::ON:
        return LogicalCheckpointManager::GetInstance(checkpointing_thread_count_);
      
      default:
        return CheckpointManager::GetInstance();
    }
  }

  static void Configure(const int thread_count = 1) {
    if (thread_count == 0) {
      checkpointing_type_ = CheckpointingType::OFF;
    } else {
      checkpointing_type_ = CheckpointingType::ON;
      checkpointing_thread_count_ = thread_count;
    }
  }

  inline static CheckpointingType GetCheckpointingType() { return logging_type_; }

private:
  // checkpointing type
  static CheckpointingType checkpointing_type_;

  static int checkpointing_thread_count_;
};

} // namespace gc
} // namespace peloton
