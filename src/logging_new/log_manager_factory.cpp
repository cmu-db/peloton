//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager_factory.cpp
//
// Identification: src/logging/log_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "logging_new/log_manager_factory.h"

namespace peloton {
namespace logging {

LoggingType LogManagerFactory::logging_type_ = LoggingType::ON;

int LogManagerFactory::logging_thread_count_ = 1;

}  // namespace gc
}  // namespace peloton
