//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_logger.cpp
//
// Identification: src/backend/logging/backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"
#include "backend/logging/log_record.h"

namespace peloton {
namespace logging {

BackendLogger::BackendLogger() { logger_type = LOGGER_TYPE_BACKEND; }

BackendLogger::~BackendLogger() {}

/**
 * @brief Return the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *BackendLogger::GetBackendLogger(LoggingType logging_type) {
  BackendLogger *backend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    backend_logger = new WriteAheadBackendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    backend_logger = new WriteBehindBackendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return backend_logger;
}

}  // namespace logging
}
