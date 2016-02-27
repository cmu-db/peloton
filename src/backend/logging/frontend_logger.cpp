/*-------------------------------------------------------------------------
 *
 * frontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/frontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <thread>

#include "backend/common/logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"

// configuration for testing
int64_t peloton_wait_timeout = 0;

namespace peloton {
namespace logging {

FrontendLogger::FrontendLogger() {
  logger_type = LOGGER_TYPE_FRONTEND;

  if (peloton_wait_timeout != 0) {
    wait_timeout = peloton_wait_timeout;
  }
}

FrontendLogger::~FrontendLogger() {
  for (auto backend_logger : backend_loggers) {
    delete backend_logger;
  }
}

/** * @brief Return the frontend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
FrontendLogger *FrontendLogger::GetFrontendLogger(LoggingType logging_type) {
  FrontendLogger *frontendLogger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    frontendLogger = new WriteAheadFrontendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    frontendLogger = new WriteBehindFrontendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return frontendLogger;
}

/**
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {
  auto &log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Standby Mode");

  // Standby before we need to do RECOVERY
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch (log_manager.GetStatus()) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      DoRecovery();

      // Now, enter LOGGING mode
      log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING: {
      LOG_TRACE("Frontendlogger] Logging Mode");
    } break;

    default:
      break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////

  // Periodically, wake up and do logging
  while (log_manager.GetStatus() == LOGGING_STATUS_TYPE_LOGGING) {
    // Collect LogRecords from all backend loggers
    CollectLogRecordsFromBackendLoggers();

    // Flush the data to the file
    FlushLogRecords();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // force the last check to be done without waiting
  need_to_collect_new_log_records = true;

  // flush any remaining log records
  CollectLogRecordsFromBackendLoggers();
  FlushLogRecords();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Sleep Mode");

  // Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the log records from BackendLoggers
 */
void FrontendLogger::CollectLogRecordsFromBackendLoggers() {
  /*
   * Don't use "while(!need_to_collect_new_log_records)",
   * we want the frontend check all backend periodically even no backend
   * notifies.
   * So that large txn can submit its log records piece by piece
   * instead of a huge submission when the txn is committed.
   */
  if (need_to_collect_new_log_records == false) {
    auto sleep_period = std::chrono::microseconds(wait_timeout);
    std::this_thread::sleep_for(sleep_period);
  }

  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);

    // Look at the local queues of the backend loggers
    for (auto backend_logger : backend_loggers) {
      auto local_queue_size = backend_logger->GetLocalQueueSize();

      // Skip current backend_logger, nothing to do
      if (local_queue_size == 0) continue;

      // Shallow copy the log record from backend_logger to here
      for (oid_t log_record_itr = 0; log_record_itr < local_queue_size;
           log_record_itr++) {
        global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
      }

      // truncate the local queue
      backend_logger->TruncateLocalQueue(local_queue_size);
    }
  }

  need_to_collect_new_log_records = false;
}

/**
 * @brief Store backend logger
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
    backend_logger->SetConnectedToFrontend(true);
  }
}

bool FrontendLogger::RemoveBackendLogger(BackendLogger *_backend_logger) {
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    oid_t offset = 0;

    for (auto backend_logger : backend_loggers) {
      if (backend_logger == _backend_logger) {
        backend_logger->SetConnectedToFrontend(false);
        break;
      } else {
        offset++;
      }
    }

    assert(offset < backend_loggers.size());
    backend_loggers.erase(backend_loggers.begin() + offset);
  }

  return true;
}

}  // namespace logging
}
