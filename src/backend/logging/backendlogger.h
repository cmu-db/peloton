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

    LogRecord* GetLogRecord(oid_t offset);

    void Commit(void);

    bool IsWaitFlush(void) const;

    bool IsAddedFrontend(void) const;

    void AddedFrontend(void);

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    /**
     * Record log
     */
    virtual void log(LogRecord* record) = 0;

    virtual size_t GetLocalQueueSize(void) const = 0;

    virtual void Truncate(oid_t offset) = 0;

    virtual LogRecord* GetTupleRecord(LogRecordType log_record_type, 
                                      txn_id_t txn_id, 
                                      oid_t table_oid, 
                                      ItemPointer insert_location, 
                                      ItemPointer delete_location, 
                                      void* data = nullptr,
                                      oid_t db_oid = INVALID_OID) = 0;

  protected:

    // TODO change vector to list
    std::vector<LogRecord*> local_queue;

    std::mutex local_queue_mutex;

    oid_t commit_offset = 0;

    bool wait_flush = false;

    bool added_in_frontend = false;
};

}  // namespace logging
}  // namespace peloton
