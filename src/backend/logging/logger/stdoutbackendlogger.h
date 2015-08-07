/*-------------------------------------------------------------------------
 *
 * stdoutbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logger/backendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Stdout Backend Logger 
//===--------------------------------------------------------------------===//

class StdoutBackendLogger : public BackendLogger{

  public:
    StdoutBackendLogger(){ logger_type = LOGGER_TYPE_STDOUT;}

    void log(LogRecord record);
  private:
    std::vector<LogRecord> stdout_buffer;
    
};

}  // namespace logging
}  // namespace peloton
