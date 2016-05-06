//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// frontend_logger.cpp
//
// Identification: src/backend/logging/frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>

#include "backend/common/logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/checkpoint_factory.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"

// configuration for testing
extern int64_t peloton_wait_timeout;

namespace peloton {
namespace logging {

FrontendLogger::FrontendLogger()
    : checkpoint(CheckpointFactory::GetInstance()) {
  logger_type = LOGGER_TYPE_FRONTEND;

  // Set wait timeout
  wait_timeout = peloton_wait_timeout;
}

FrontendLogger::~FrontendLogger() {
  for (auto backend_logger : backend_loggers) {
    delete backend_logger;
  }
}

/** * @brief Return the frontend logger based on logging type
 * @param logging type can be write ahead logging or write behind logging
 */
FrontendLogger *FrontendLogger::GetFrontendLogger(LoggingType logging_type) {
  FrontendLogger *frontend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    frontend_logger = new WriteAheadFrontendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    frontend_logger = new WriteBehindFrontendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return frontend_logger;
}

/**
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {
  auto &log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("FrontendLogger Standby Mode");

  // Standby before we need to do RECOVERY
  log_manager.WaitForModeTransition(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch (log_manager.GetLoggingStatus()) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      LOG_INFO("Log manager: Invoking DoRecovery");
      DoRecovery();
      LOG_INFO("Log manager: DoRecovery done");

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
  while (log_manager.GetLoggingStatus() == LOGGING_STATUS_TYPE_LOGGING) {
    // Collect LogRecords from all backend loggers
    // LOG_INFO("Log manager: Invoking CollectLogRecordsFromBackendLoggers");
    CollectLogRecordsFromBackendLoggers();

    // Flush the data to the file
    // LOG_INFO("Log manager: Invoking FlushLogRecords");
    FlushLogRecords();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // flush any remaining log records
  CollectLogRecordsFromBackendLoggers();
  FlushLogRecords();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger Sleep Mode");

  // Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the log records from BackendLoggers
 */
void FrontendLogger::CollectLogRecordsFromBackendLoggers() {
  auto sleep_period = std::chrono::milliseconds(wait_timeout);
  std::this_thread::sleep_for(sleep_period);

  {
    // Look at the local queues of the backend loggers
    for (auto backend_logger : backend_loggers) {
      {
        std::lock_guard<std::mutex> lock(backend_logger->local_queue_mutex);

        auto local_queue_size = backend_logger->local_queue.size();

        // Skip current backend_logger, nothing to do
        if (local_queue_size == 0) continue;

        // Move the log record from backend_logger to here
        for (oid_t log_record_itr = 0; log_record_itr < local_queue_size;
             log_record_itr++) {
          LOG_INFO("Found a log record to push in global queue");
          global_queue.push_back(
              std::move(backend_logger->local_queue[log_record_itr]));
        }

        // cleanup the local queue
        backend_logger->local_queue.clear();
      }
    }
  }
}

/**
 * @brief Store backend logger
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  // Add backend logger to the list of backend loggers
  backend_loggers.push_back(backend_logger);
}

}  // namespace logging
}
