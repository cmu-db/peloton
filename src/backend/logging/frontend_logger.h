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
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unistd.h>
#include <map>
#include <thread>

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/checkpoint.h"

namespace peloton {
namespace logging {

class Checkpoint;

//===--------------------------------------------------------------------===//
// Frontend Logger
//===--------------------------------------------------------------------===//

class FrontendLogger : public Logger {
 public:
  FrontendLogger();

  ~FrontendLogger();

  static FrontendLogger *GetFrontendLogger(LoggingType logging_type);

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

  size_t GetFsyncCount() const { return fsync_count; }

  void ReplayLog(const char *, size_t len);

 protected:
  // Associated backend loggers
  std::vector<BackendLogger*> backend_loggers;

  // Global queue
  std::vector<std::unique_ptr<LogRecord>> global_queue;

  // period with which it collects log records from backend loggers
  // (in milliseconds)
  int64_t wait_timeout;

  // stats
  size_t fsync_count = 0;

  // checkpoint
  Checkpoint &checkpoint;
};

}  // namespace logging
}  // namespace peloton
