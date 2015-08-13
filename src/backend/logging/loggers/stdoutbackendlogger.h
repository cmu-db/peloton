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

class StdoutBackendLogger : public BackendLogger{

  public:

    static StdoutBackendLogger* GetInstance(void);

    void Insert(LogRecord* record);

    void Delete(LogRecord* record);

    void Update(LogRecord* record);

    void Commit(void);

    void Truncate(oid_t offset);

    LogRecord* GetLogRecord(oid_t offset);

  private:

    StdoutBackendLogger(){ logging_type = LOGGING_TYPE_STDOUT;}

    std::vector<LogRecord*> stdout_local_queue;

    std::mutex stdout_local_queue_mutex;

};

}  // namespace logging
}  // namespace peloton
