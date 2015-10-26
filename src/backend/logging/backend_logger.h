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

#include <vector>
#include <mutex>
#include <condition_variable>

#include "backend/logging/logger.h"
#include "backend/logging/log_record.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Backend Logger 
//===--------------------------------------------------------------------===//

class BackendLogger : public Logger{

  public:

    BackendLogger() {logger_type = LOGGER_TYPE_BACKEND;}

    ~BackendLogger(){
      for(auto log_record : local_queue){
        delete log_record;
      }
    }

    static BackendLogger* GetBackendLogger(LoggingType logging_type);

    // Get the log record in the local queue at given offset
    LogRecord* GetLogRecord(oid_t offset);

    void Commit(void);

    bool IsConnectedToFrontend(void) const;

    void SetConnectedToFrontend(bool isConnected);

    // Truncate the log file at given offset
    void TruncateLocalQueue(oid_t offset);

    void WaitForFlushing(void);
    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    /**
     * Record log
     */

    // Log the given record
    virtual void Log(LogRecord* record) = 0;

    virtual size_t GetLocalQueueSize(void) = 0;

    // Construct a log record with tuple information
    virtual LogRecord* GetTupleRecord(LogRecordType log_record_type, 
                                      txn_id_t txn_id, 
                                      oid_t table_oid, 
                                      ItemPointer insert_location, 
                                      ItemPointer delete_location, 
                                      void* data = nullptr,
                                      oid_t db_oid = INVALID_OID) = 0;

  protected:
    bool IsWaitingForFlushing(void);

    std::vector<LogRecord*> local_queue;
    std::mutex local_queue_mutex;

    // wait for the frontend to flush
    // need to ensure synchronous commit
    bool wait_for_flushing = false;
    // Used for notify any waiting thread that backend is flushed
    std::mutex flush_notify_mutex;
    std::condition_variable flush_notify_cv;

    // is this backend connected to frontend ?
    bool connected_to_frontend = false;
};

}  // namespace logging
}  // namespace peloton
