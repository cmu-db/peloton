/*-------------------------------------------------------------------------
 *
 * stdoutfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logrecord.h"
#include "backend/logging/logger/frontendlogger.h"

namespace peloton {
namespace logging {

static std::vector<LogRecord> stdout_buffer;

static std::mutex stdout_buffer_mutex;

//===--------------------------------------------------------------------===//
// Stdout Frontend Logger 
//===--------------------------------------------------------------------===//

class StdoutFrontendLogger : public FrontendLogger{
  public:
    StdoutFrontendLogger(){ logger_type = LOGGER_TYPE_STDOUT;}

    void MainLoop(void) const;

    void flush(void) const;

  private:
    oid_t buffer_size = 10;
    
    size_t GetBufferSize() const;
};

}  // namespace logging
}  // namespace peloton
