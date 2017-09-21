//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager_factory.h
//
// Identification: src/include/logging/log_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_manager.h"
#include "logging/wal_log_manager.h"
#include "logging/dummy_log_manager.h"

namespace peloton {
namespace logging {

class LogManagerFactory {
 public:

  static LogManager& GetInstance() {
    switch (logging_type_) {

      case LoggingType::ON:
        return WalLogManager::GetInstance();
      
      default:
        return DummyLogManager::GetInstance();
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

} // namespace logging
} // namespace peloton
