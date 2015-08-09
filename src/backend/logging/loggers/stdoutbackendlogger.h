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

#include "backend/logging/backendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Stdout Backend Logger 
//===--------------------------------------------------------------------===//

static std::mutex stdout_buffer_mutex;

class StdoutBackendLogger : public BackendLogger{

  public:

    //TODO :: It alaways returns different value, make this as a singleton
    StdoutBackendLogger(){ logging_type = LOGGING_TYPE_STDOUT;}


    void Log(LogRecord record);

    void Commit(void);

    void Truncate(oid_t offset);

    LogRecord GetLogRecord(oid_t offset);

  private:

    // TODO change vector to list
    std::vector<LogRecord> stdout_buffer;
    
};

}  // namespace logging
}  // namespace peloton
