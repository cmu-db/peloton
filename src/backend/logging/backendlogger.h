/*-------------------------------------------------------------------------
 *
 * backendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/backendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logrecord.h"
#include "backend/logging/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Backend Logger 
//===--------------------------------------------------------------------===//

class BackendLogger : public Logger{

  public:

    BackendLogger() {logger_type = LOGGER_TYPE_BACKEND;}

    static BackendLogger* GetBackendLogger(LoggingType logging_type);

    oid_t GetCommitOffset(void) const;

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    virtual void Log(LogRecord record) = 0;

    virtual void Commit(void) = 0;

    virtual void Truncate(oid_t offset) = 0;

    virtual LogRecord GetLogRecord(oid_t offset) = 0;

  protected:

    oid_t commit_offset = 0;

};

}  // namespace logging
}  // namespace peloton
