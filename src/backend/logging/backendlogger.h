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

    void Truncate(oid_t offset);

    LogRecord* GetLogRecord(oid_t offset);

    void Commit(void);

    size_t GetLocalQueueSize(void) const;

    bool IsWaitFlush(void) const;

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    /**
     * Record log
     */
    virtual void Insert(LogRecord* record) = 0;

    virtual void Delete(LogRecord* record) = 0;

    virtual void Update(LogRecord* record) = 0;

  protected:

    // TODO change vector to list
    std::vector<LogRecord*> local_queue;

    std::mutex local_queue_mutex;

    oid_t commit_offset = 0;

    bool wait_flush = false;
};

}  // namespace logging
}  // namespace peloton
