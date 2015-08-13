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

#include <vector>
#include <mutex>

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

    /**
     * Record log
     */
    virtual void Insert(LogRecord* record) = 0;

    virtual void Delete(LogRecord* record) = 0;

    virtual void Update(LogRecord* record) = 0;

    /**
     * Commit locally so that FrontendLogger can collect LogRecord from here
     */
    virtual void Commit(void) = 0;

    /**
     * Remove LogRecord that already collected by FrontendLogger
     */
    virtual void Truncate(oid_t offset) = 0;

    /**
     * Return LogRecord with offset
     */
    virtual LogRecord* GetLogRecord(oid_t offset) = 0;

  protected:

    oid_t commit_offset = 0;

};

}  // namespace logging
}  // namespace peloton
