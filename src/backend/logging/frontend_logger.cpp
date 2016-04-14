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
  auto sleep_period = std::chrono::microseconds(wait_timeout);
  std::this_thread::sleep_for(sleep_period);

  {
    cid_t max_possible_commit_id = MAX_CID;

    // TODO: handle edge cases here (backend logger has not yet sent a log
    // message)

    // Look at the local queues of the backend loggers
    while (backend_loggers_lock.test_and_set(std::memory_order_acquire))
      ;
    LOG_TRACE("Collect log buffers from %lu backend loggers",
              backend_loggers.size());
    for (auto backend_logger : backend_loggers) {
      {
        auto cid = backend_logger->PrepareLogBuffers();
        auto &log_buffers = backend_logger->GetLogBuffers();
        auto log_buffer_size = log_buffers.size();

        // update max_possible_commit_id with the latest buffer
        if (cid != INVALID_CID) {
          LOG_INFO("Found %lu log buffers to persist with commit id: %lu",
                   log_buffer_size, cid);
          max_possible_commit_id = std::min(cid, max_possible_commit_id);
        }

        // Skip current backend_logger, nothing to do
        if (log_buffer_size == 0) continue;

        // Move the log record from backend_logger to here
        for (oid_t log_record_itr = 0; log_record_itr < log_buffer_size;
             log_record_itr++) {
          // copy to front end logger
          global_queue.push_back(std::move(log_buffers[log_record_itr]));
        }
        // cleanup the local queue
        log_buffers.clear();
      }
    }

    if (max_possible_commit_id != MAX_CID) {
      // LOG_INFO("compare max_possible_commit_id %lu >= max_collected_commit_id
      // %lu ",
      //		  max_possible_commit_id, max_collected_commit_id);
      assert(max_possible_commit_id >= max_collected_commit_id);
      max_collected_commit_id = max_possible_commit_id;
    }
    backend_loggers_lock.clear(std::memory_order_release);
  }
}

cid_t FrontendLogger::GetMaxFlushedCommitId() { return max_flushed_commit_id; }

void FrontendLogger::SetBackendLoggerLoggedCid(BackendLogger &bel) {
  while (backend_loggers_lock.test_and_set(std::memory_order_acquire))
    ;
  LOG_INFO("FrontendLogger::GetMaxFlushedCommitId gonna set backend logger max collected commit id to %d", (int)max_collected_commit_id);
  bel.SetHighestLoggedCommitId(max_collected_commit_id);
  backend_loggers_lock.clear(std::memory_order_release);
}

/**
 * @brief Add backend logger to the list of backend loggers
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  // Grant empty buffers
  for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
    std::unique_ptr<LogBuffer> buffer(new LogBuffer(backend_logger));
    backend_logger->GrantEmptyBuffer(std::move(buffer));
  }
  // Add backend logger to the list of backend loggers
  while (backend_loggers_lock.test_and_set(std::memory_order_acquire))
    ;
  LOG_INFO("FrontendLogger::AddBackendLogger gonna set backend logger max collected commit id to %d", (int)max_collected_commit_id);
  backend_logger->SetHighestLoggedCommitId(max_collected_commit_id);
  backend_loggers.push_back(backend_logger);
  backend_loggers_lock.clear(std::memory_order_release);
}

}  // namespace logging
}
