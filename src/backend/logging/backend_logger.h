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
  virtual void Log(LogRecord *record) = 0;

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

  virtual std::pair<cid_t, cid_t> PrepareLogBuffers() = 0;

  // Grant an empty buffer to use
  virtual void GrantEmptyBuffer(std::unique_ptr<LogBuffer>) = 0;

 protected:
  // XXX the lock for the buffer being used currently
  Spinlock log_buffer_lock;

  std::vector<std::unique_ptr<LogBuffer>> local_queue;

  cid_t highest_logged_commit_message = INVALID_CID;

  cid_t logging_cid_lower_bound = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
