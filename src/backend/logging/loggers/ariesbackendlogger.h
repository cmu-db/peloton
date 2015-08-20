/*-------------------------------------------------------------------------
 *
 * ariesbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Aries Backend Logger 
//===--------------------------------------------------------------------===//


class AriesBackendLogger : public BackendLogger{

  public:
    AriesBackendLogger(const AriesBackendLogger &) = delete;
    AriesBackendLogger &operator=(const AriesBackendLogger &) = delete;
    AriesBackendLogger(AriesBackendLogger &&) = delete;
    AriesBackendLogger &operator=(AriesBackendLogger &&) = delete;

    static AriesBackendLogger* GetInstance(void);

    void Insert(LogRecord* record);

    void Delete(LogRecord* record);

    void Update(LogRecord* record);

    void Commit(bool coerce = false);

    void Truncate(oid_t offset);

    LogRecord* GetLogRecord(oid_t offset);

  private:

    AriesBackendLogger(){ logging_type = LOGGING_TYPE_STDOUT;}

    // TODO change vector to list
    std::vector<LogRecord*> aries_local_queue;

    std::mutex aries_local_queue_mutex;

    // FIXME :: Hard coded
    size_t aries_local_queue_size = 5;

};

}  // namespace logging
}  // namespace peloton
