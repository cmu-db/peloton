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
  for (auto log_record : local_queue) {
    delete log_record;
  }
}

/**
 * @brief Return the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *BackendLogger::GetBackendLogger(LoggingType logging_type) {
  BackendLogger *backendLogger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    backendLogger = WriteAheadBackendLogger::GetInstance();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    backendLogger = WriteBehindBackendLogger::GetInstance();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return backendLogger;
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
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void BackendLogger::TruncateLocalQueue(oid_t offset) {
  // Lock notify first, make sure is_wait_for_flush will not return premature
  std::lock_guard<std::mutex> lock(flush_notify_mutex);
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    // cleanup the queue
    local_queue.erase(local_queue.begin(), local_queue.begin() + offset);
  }

  // let's wait for the frontend logger to flush !
  // the frontend logger will call our Commit to reset it.
  wait_for_flushing = true;
}

/**
 * @brief Get the LogRecord with offset
 * @param offset
 */
LogRecord *BackendLogger::GetLogRecord(oid_t offset) {
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    assert(offset < local_queue.size());
    return local_queue[offset];
  }
}

/**
 * @brief if we still have log record in local queue or waiting for flushing
 * @return true otherwise false
 */
bool BackendLogger::IsWaitingForFlushing(void) {
  // Sometimes, the backend logger has some log records even if
  // wait_for_flushing is false.
  // For example, if backend logger enqueues the log record right after
  // frontend logger collect data and not truncated yet.
  if (wait_for_flushing || GetLocalQueueSize() > 0) {
    return true;
  } else {
    return false;
  }
}

bool BackendLogger::IsConnectedToFrontend(void) const {
  return connected_to_frontend;
}

void BackendLogger::SetConnectedToFrontend(bool isConnected) {
  connected_to_frontend = isConnected;
}

void BackendLogger::WaitForFlushing(void) {
  std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);
  while (IsWaitingForFlushing()) {
    flush_notify_cv.wait(wait_lock);
  }
}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t BackendLogger::GetLocalQueueSize(void) {
  std::lock_guard<std::mutex> lock(local_queue_mutex);
  return local_queue.size();
}

}  // namespace logging
}
