//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger.h
//
// Identification: src/backend/logging/logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger
//===--------------------------------------------------------------------===//
class Logger {
 public:
  LoggingType GetLoggingType(void) const;

  LoggerType GetLoggerType(void) const;

  virtual ~Logger(void){};

 protected:
  // Type of logging -- aries, peloton etc.
  LoggingType logging_type = LOGGING_TYPE_INVALID;

  // Type of logger -- frontend, backend etc.
  LoggerType logger_type = LOGGER_TYPE_INVALID;
};

}  // namespace logging
}  // namespace peloton
