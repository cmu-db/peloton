/*-------------------------------------------------------------------------
 *
 * backendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/backendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend_logger.h"

#include "backend/common/logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"

namespace peloton {
namespace logging {

BackendLogger::~BackendLogger() {

  // Wait for flushing
  WaitForFlushing();

}

/**
 * @brief Return the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *BackendLogger::GetBackendLogger(LoggingType logging_type) {
  BackendLogger *backend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    backend_logger = WriteAheadBackendLogger::GetInstance();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    backend_logger = WriteBehindBackendLogger::GetInstance();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return backend_logger;
}

/**
 * @brief set the wait flush to false
 */
void BackendLogger::Commit(void) {
  std::lock_guard<std::mutex> lock(flush_notify_mutex);
  // Only need to commit if they are waiting for us to flush
  if (wait_for_flushing) {
    wait_for_flushing = false;
    flush_notify_cv.notify_all();
  }
}

/**
 * @brief if we still have log record in local queue or waiting for flushing
 * @return true otherwise false
 */
bool BackendLogger::IsWaitingForFlushing(void) {
  // Sometimes, the backend logger has some log records even if
  // wait_for_flushing is false. For example, if backend logger enqueues the log
  // record right after the frontend logger collected data and not truncated yet.
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    if (wait_for_flushing || local_queue.size() > 0) {
      return true;
    } else {
      return false;
    }
  }
}

void BackendLogger::WaitForFlushing(void) {
  std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);
  while (IsWaitingForFlushing()) {
    flush_notify_cv.wait(wait_lock);
  }
}

}  // namespace logging
}
