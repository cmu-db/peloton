//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// frontend_logger.h
//
// Identification: src/backend/logging/frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <unistd.h>
#include <map>
#include <thread>

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/buffer_pool.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/checkpoint.h"

namespace peloton {
namespace logging {
//===--------------------------------------------------------------------===//
// Frontend Logger
//===--------------------------------------------------------------------===//

class FrontendLogger : public Logger {
 public:
  FrontendLogger();

  ~FrontendLogger();

  static FrontendLogger *GetFrontendLogger(LoggingType logging_type,
                                           bool test_mode = false);

  void MainLoop(void);

  void CollectLogRecordsFromBackendLoggers(void);

  void AddBackendLogger(BackendLogger *backend_logger);

  //===--------------------------------------------------------------------===//
  // Virtual Functions
  //===--------------------------------------------------------------------===//

  // Flush collected LogRecords
  virtual void FlushLogRecords(void) = 0;

  // Restore database
  virtual void DoRecovery(void) = 0;

  virtual void SetLoggerID(int) = 0;

  virtual void RecoverIndex(void) = 0;

  size_t GetFsyncCount() const { return fsync_count; }

  void ReplayLog(const char *, size_t len);

  cid_t GetMaxFlushedCommitId();

  void SetMaxFlushedCommitId(cid_t cid);

  void SetBackendLoggerLoggedCid(BackendLogger &bel);

  cid_t GetMaxDelimiterForRecovery() { return max_delimiter_for_recovery; }

  void SetIsDistinguishedLogger(bool flag) { is_distinguished_logger = flag; }

  void UpdateGlobalMaxFlushId();

  // reset the frontend logger to its original state (for testing
  void Reset() {
    backend_loggers_lock.Lock();
    fsync_count = 0;
    max_flushed_commit_id = 0;
    max_collected_commit_id = 0;
    max_seen_commit_id = 0;
    global_queue.clear();
    backend_loggers.clear();
    backend_loggers_lock.Unlock();
  }

 protected:
  // Associated backend loggers
  std::vector<BackendLogger *> backend_loggers;

  // Global queue
  std::vector<std::unique_ptr<LogBuffer>> global_queue;

  // To synch the status
  Spinlock backend_loggers_lock;

  // period with which it collects log records from backend loggers
  // (in milliseconds)
  int64_t wait_timeout;

  // stats
  size_t fsync_count = 0;

  cid_t max_flushed_commit_id = 0;

  cid_t max_collected_commit_id = 0;

  cid_t max_delimiter_for_recovery = 0;

  cid_t max_seen_commit_id = 0;

  bool is_distinguished_logger = false;
};

}  // namespace logging
}  // namespace peloton
