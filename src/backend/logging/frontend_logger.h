/*-------------------------------------------------------------------------
 *
 * frontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/frontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

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

namespace peloton {
namespace logging {

class BackendLogger;
class LogRecord;

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

  size_t GetFsyncCount() const {
    return fsync_count;
  }

 protected:
  // Associated backend loggers
  std::vector<BackendLogger*> backend_loggers;

  // Global queue
  std::vector<std::unique_ptr<LogRecord>> global_queue;

  // period with which it collects log records from backend loggers
  // (in microseconds)
  int64_t wait_timeout;

  // stats
  size_t fsync_count = 0;
};

}  // namespace logging
}  // namespace peloton
