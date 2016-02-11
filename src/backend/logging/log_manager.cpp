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

#include "backend/logging/log_manager.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the singleton log manager instance
 */
LogManager &LogManager::GetInstance() {
  static LogManager log_manager;
  return log_manager;
}

LogManager::LogManager() {}

LogManager::~LogManager() {}

/**
 * @brief Standby logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartStandbyMode() {
  // If frontend logger doesn't exist
  if (frontend_logger == nullptr) {
    frontend_logger = FrontendLogger::GetFrontendLogger(peloton_logging_mode);
  }

  // If frontend logger still doesn't exist, then we disabled logging
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

bool LogManager::IsInLoggingMode() {
  // Check the logging status
  if (logging_status == LOGGING_STATUS_TYPE_LOGGING)
    return true;
  else
    return false;
}

void LogManager::TerminateLoggingMode() {
  SetLoggingStatus(LOGGING_STATUS_TYPE_TERMINATE);

  // We set the frontend logger status to Terminate
  // And, then we wait for the transition to Sleep mode
  WaitForMode(LOGGING_STATUS_TYPE_SLEEP, true);
}

void LogManager::WaitForMode(LoggingStatus logging_status_, bool is_equal) {
  // wait for mode change
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
  WaitForMode(LOGGING_STATUS_TYPE_RECOVERY, false);

  LOG_INFO("Wait until frontend logger escapes main loop..");

  // Wait for the frontend logger to enter sleep mode
  TerminateLoggingMode();

  LOG_INFO("Escaped from MainLoop");

  // Remove the frontend logger
  if (RemoveFrontendLogger()) {
    ResetLoggingStatusMap();
    LOG_INFO("Terminated successfully");
    return true;
  }

  return false;
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
  BackendLogger *backend_logger = nullptr;

  // Check whether the frontend logger exists or not
  // if so, create backend logger and store it in frontend logger
  {
    // If frontend logger exists
    if (frontend_logger != nullptr) {
      backend_logger = BackendLogger::GetBackendLogger(peloton_logging_mode);
      if (!backend_logger->IsConnectedToFrontend()) {
        frontend_logger->AddBackendLogger(backend_logger);
      }
    }
  }

  if (frontend_logger == nullptr) {
    LOG_ERROR("Frontend logger doesn't exist!!");
  }

  return backend_logger;
}

bool LogManager::RemoveBackendLogger(BackendLogger *backend_logger) {
  bool status = false;

  // Check whether the frontend logger exists or not
  // if so, remove backend logger otherwise return false
  {
    // If frontend logger exists
    if (frontend_logger != nullptr) {
      status = frontend_logger->RemoveBackendLogger(backend_logger);
    }
  }

  return status;
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the frontend logger otherwise nullptr
 */
FrontendLogger *LogManager::GetFrontendLogger() { return frontend_logger; }

bool LogManager::RemoveFrontendLogger() {
  // Erase frontend logger
  delete frontend_logger;

  // Reset
  frontend_logger = nullptr;

  return true;
}

void LogManager::ResetLoggingStatusMap() {
  // Remove status for the specific logging type
  {
    logging_status = LOGGING_STATUS_TYPE_INVALID;

    // in case any threads are waiting...
    logging_status_cv.notify_all();
  }
}

size_t LogManager::ActiveFrontendLoggerCount(void) {
  return (frontend_logger != nullptr);
}

/**
 * @brief mark Peloton is ready, so that frontend logger can start logging
 */
LoggingStatus LogManager::GetStatus() {
  // Get the status from the map
  return logging_status;
}

void LogManager::SetLoggingStatus(LoggingStatus logging_status_) {
  // Set the status in the log manager map
  {
    std::lock_guard<std::mutex> lock(logging_status_mutex);

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
  // Check if we need to build a log file name
  if (log_file_name.empty()) {
    // If peloton_log_directory is specified
    if (peloton_log_directory != nullptr) {
      log_file_name = std::string(peloton_log_directory) + "/" + "peloton.log";
    }
    // Else save it in tmp directory
    else {
      log_file_name = "/tmp/peloton.log";
    }
  }

  return log_file_name;
}

}  // namespace logging
}  // namespace peloton
