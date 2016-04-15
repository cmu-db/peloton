//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_logger.h
//
// Identification: src/backend/logging/backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
  BackendLogger();

  ~BackendLogger();

  static BackendLogger *GetBackendLogger(LoggingType logging_type);

  void FinishedFlushing(void);

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
                                    const void *data = nullptr) = 0;

 protected:

  std::vector<std::unique_ptr<LogRecord>> local_queue;
  std::mutex local_queue_mutex;

  // Used for notify any waiting thread that backend is flushed
  std::mutex flush_notify_mutex;
  std::condition_variable flush_notify_cv;
};

}  // namespace logging
}  // namespace peloton
