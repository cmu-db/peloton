/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger 
//===--------------------------------------------------------------------===//

class Logger{

  public:
    LoggingType GetLoggingType(void) const;

    LoggerType GetLoggerType(void) const;

    virtual ~Logger(void){};

  protected:
    LoggingType logging_type = LOGGING_TYPE_INVALID;

    LoggerType logger_type = LOGGER_TYPE_INVALID;

};

}  // namespace logging
}  // namespace peloton
