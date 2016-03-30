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

class BackendLogger : public Logger {
  friend class FrontendLogger;

 public:
  BackendLogger() { logger_type = LOGGER_TYPE_BACKEND; }

  ~BackendLogger();

  static BackendLogger *GetBackendLogger(LoggingType logging_type);

  void Commit(void);

  void WaitForFlushing(void);

  //===--------------------------------------------------------------------===//
  // Virtual Functions
  //===--------------------------------------------------------------------===//

  // Log the given record
  virtual void Log(LogRecord *record) = 0;

  // Construct a log record with tuple information
  virtual LogRecord *GetTupleRecord(LogRecordType log_record_type,
                                    txn_id_t txn_id,
                                    oid_t table_oid,
                                    oid_t db_oid,
                                    ItemPointer insert_location,
                                    ItemPointer delete_location,
                                    void *data = nullptr) = 0;

 protected:
  bool IsWaitingForFlushing(void);

  std::vector<std::unique_ptr<LogRecord>> local_queue;
  std::mutex local_queue_mutex;

  // wait for the frontend to flush
  // need to ensure synchronous commit
  bool wait_for_flushing = false;

  // Used for notify any waiting thread that backend is flushed
  std::mutex flush_notify_mutex;
  std::condition_variable flush_notify_cv;
};

}  // namespace logging
}  // namespace peloton
