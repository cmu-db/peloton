//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager_factory.h
//
// Identification: src/include/concurrency/log_manager_factory.h
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
    switch (logging_type_) {

      case LoggingType::ON:
        return LogicalCheckpointManager::GetInstance(logging_thread_count_);
      
      default:
        return CheckpointManager::GetInstance();
    }
  }

  static void Configure(const int thread_count = 1) {
    if (thread_count == 0) {
      logging_type_ = LoggingType::OFF;
    } else {
      logging_type_ = LoggingType::ON;
      logging_thread_count_ = thread_count;
    }
  }

  inline static LoggingType GetLoggingType() { return logging_type_; }

private:
  // checkpointing type
  static LoggingType logging_type_;

  static int logging_thread_count_;
};
} // namespace gc
} // namespace peloton
