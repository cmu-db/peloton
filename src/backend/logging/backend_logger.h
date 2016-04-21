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

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer.h"
#include "backend/common/platform.h"
#include "backend/logging/circular_buffer_pool.h"

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

  //===--------------------------------------------------------------------===//
  // Virtual Functions
  //===--------------------------------------------------------------------===//

  // Log the given record
  void Log(LogRecord *record);

  // Construct a log record with tuple information
  virtual LogRecord *GetTupleRecord(LogRecordType log_record_type,
                                    txn_id_t txn_id, oid_t table_oid,
                                    oid_t db_oid, ItemPointer insert_location,
                                    ItemPointer delete_location,
                                    const void *data = nullptr) = 0;

  // cid_t GetHighestLoggedCommitId() { return highest_logged_commit_id; };

  void SetLoggingCidLowerBound(cid_t cid) {
    // XXX bad synchronization practice
    log_buffer_lock.Lock();

    logging_cid_lower_bound = cid;

    highest_logged_commit_message = INVALID_CID;

    log_buffer_lock.Unlock();
  }

  // FIXME The following methods should be exposed to FrontendLogger only
  // Collect all log buffers to be persisted
  std::vector<std::unique_ptr<LogBuffer>> &GetLogBuffers() {
    return local_queue;
  }

  std::pair<cid_t, cid_t> PrepareLogBuffers();

  // Grant an empty buffer to use
  void GrantEmptyBuffer(std::unique_ptr<LogBuffer>);

  // Set FrontendLoggerID
  void SetFrontendLoggerID(int id) { frontend_logger_id = id; }

  // Get FrontendLoggerID
  int GetFrontendLoggerID() { return frontend_logger_id; }

 protected:
  // XXX the lock for the buffer being used currently
  Spinlock log_buffer_lock;

  std::vector<std::unique_ptr<LogBuffer>> local_queue;

  cid_t highest_logged_commit_message = INVALID_CID;

  int frontend_logger_id;

  cid_t logging_cid_lower_bound = INVALID_CID;

  cid_t max_log_id_buffer = 0;

  // temporary serialization buffer
  CopySerializeOutput output_buffer;

  // the current buffer
  std::unique_ptr<LogBuffer> log_buffer_;

  // the pool of available buffers
  std::unique_ptr<BufferPool> available_buffer_pool_;

  // the pool of buffers to persist
  std::unique_ptr<BufferPool> persist_buffer_pool_;
};

}  // namespace logging
}  // namespace peloton
