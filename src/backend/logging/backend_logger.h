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
#include "backend/logging/log_buffer.h"

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
                                    void *data = nullptr) = 0;

  // Collect all log buffers to be persisted
  virtual std::vector<std::unique_ptr<LogBuffer>> &CollectLogBuffers() = 0;

  // Grant an empty buffer to use
  virtual void GrantEmptyBuffer(std::unique_ptr<LogBuffer>) = 0;

  cid_t GetHighestLoggedCommitId() { return highest_logged_commit_id; };

  void SetHighestLoggedCommitId(cid_t cid) { highest_logged_commit_id = cid; };

 protected:
  std::vector<std::unique_ptr<LogBuffer>> log_buffers_to_collect;

  cid_t highest_logged_commit_id = 0;

  cid_t highest_flushed_cid = 0;
};

}  // namespace logging
}  // namespace peloton
