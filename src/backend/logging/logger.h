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
#include "backend/logging/logproxy.h"
#include "backend/logging/logrecord.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger 
//===--------------------------------------------------------------------===//

class Logger{

  public:
    Logger() = delete;

    Logger(LoggerId logger_id, LogProxy *proxy) 
    : logger_id(logger_id), proxy(proxy) {};
    
    void logging_MainLoop(void);

    void log(LogRecord record);

    void flush(void);

  private:
    LoggerId logger_id = LOGGER_ID_INVALID;

    LogProxy *proxy;
};

}  // namespace logging
}  // namespace peloton
