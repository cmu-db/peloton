//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger.h
//
// Identification: src/include/logging/logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "type/types.h"
#include "common/logger.h"

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
  LoggingType logging_type = LoggingType::INVALID;

  // Type of logger -- frontend, backend etc.
  LoggerType logger_type = LoggerType::INVALID;
};

}  // namespace logging
}  // namespace peloton
