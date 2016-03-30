/*-------------------------------------------------------------------------
 *
 * logmanager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <condition_variable>
#include <memory>

#include "backend/logging/log_manager.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

#define LOG_FILE_NAME "wal.log"

// Each thread gets a backend logger
thread_local static BackendLogger* raw_backend_logger = nullptr;

LogManager::LogManager() {
}

LogManager::~LogManager() {
}

/**
 * @brief Return the singleton log manager instance
 */
LogManager &LogManager::GetInstance() {
  static LogManager log_manager;
  return log_manager;
}

/**
 * @brief Standby logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartStandbyMode() {
  // If frontend logger doesn't exist
  if (frontend_logger == nullptr) {
    frontend_logger.reset(FrontendLogger::GetFrontendLogger(peloton_logging_mode));
  }

  // If frontend logger still doesn't exist, then we have disabled logging
  if (frontend_logger == nullptr) {
    LOG_INFO("We have disabled logging");
    return;
  }

  // Toggle status in log manager map
  SetLoggingStatus(LOGGING_STATUS_TYPE_STANDBY);

  // Launch the frontend logger's main loop
  frontend_logger->MainLoop();
}

void LogManager::StartRecoveryMode() {
  // Toggle the status after STANDBY
  SetLoggingStatus(LOGGING_STATUS_TYPE_RECOVERY);
}

void LogManager::TerminateLoggingMode() {
  SetLoggingStatus(LOGGING_STATUS_TYPE_TERMINATE);

  // We set the frontend logger status to Terminate
  // And, then we wait for the transition to sleep mode
  WaitForModeTransition(LOGGING_STATUS_TYPE_SLEEP, true);
}

void LogManager::WaitForModeTransition(LoggingStatus logging_status_, bool is_equal) {
  // Wait for mode transition
  {
    std::unique_lock<std::mutex> wait_lock(logging_status_mutex);

    while ((!is_equal && logging_status == logging_status_) ||
        (is_equal && logging_status != logging_status_)) {
      logging_status_cv.wait(wait_lock);
    }
  }
}

/**
 * @brief stopping logging
 * Disconnect backend loggers and frontend logger from log manager
 */
bool LogManager::EndLogging() {
  // Wait if current status is recovery
  WaitForModeTransition(LOGGING_STATUS_TYPE_RECOVERY, false);

  LOG_INFO("Wait until frontend logger escapes main loop..");

  // Wait for the frontend logger to enter sleep mode
  TerminateLoggingMode();

  LOG_INFO("Escaped from MainLoop");

  // Remove the frontend logger
  SetLoggingStatus(LOGGING_STATUS_TYPE_INVALID);
  LOG_INFO("Terminated successfully");

  return true;
}

//===--------------------------------------------------------------------===//
// Utility Functions
//===--------------------------------------------------------------------===//

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *LogManager::GetBackendLogger() {
  assert(frontend_logger != nullptr);

  // Check whether the backend logger exists or not
  // if not, create a backend logger and store it in frontend logger
  if (raw_backend_logger == nullptr) {
    raw_backend_logger = BackendLogger::GetBackendLogger(peloton_logging_mode);
    frontend_logger->AddBackendLogger(raw_backend_logger);
  }

  return raw_backend_logger;
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the frontend logger otherwise nullptr
 */
FrontendLogger *LogManager::GetFrontendLogger() {
  return frontend_logger.get();
}

bool LogManager::ContainsFrontendLogger(void) {
  return (frontend_logger != nullptr);
}

/**
 * @brief mark Peloton is ready, so that frontend logger can start logging
 */
LoggingStatus LogManager::GetLoggingStatus() {
  // Get the logging status
  return logging_status;
}

void LogManager::SetLoggingStatus(LoggingStatus logging_status_) {
  {
    std::lock_guard<std::mutex> wait_lock(logging_status_mutex);

    // Set the status in the log manager map
    logging_status = logging_status_;

    // notify everyone about the status change
    logging_status_cv.notify_all();
  }
}

void LogManager::SetLogFileName(std::string log_file) {
  log_file_name = log_file;
}

// XXX change to read configuration file
std::string LogManager::GetLogFileName(void) {
  assert(log_file_name.empty() == false);
  return log_file_name;
}

}  // namespace logging
}  // namespace peloton
