/*-------------------------------------------------------------------------
 *
 * logmanager.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logmanager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logger.h"
#include <mutex>
#include <map>
#include <vector>
#include <condition_variable>

#include "backend_logger.h"
#include "frontend_logger.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LoggingType peloton_logging_mode;

// Directory for peloton logs
extern char *peloton_log_directory;

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

// Logging basically refers to the PROTOCOL -- like aries or peloton
// Logger refers to the implementation -- like frontend or backend
// Transition diagram :: standby -> recovery -> logging -> terminate -> sleep

/**
 * Global Log Manager
 */
class LogManager {
 public:
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

  // global singleton
  static LogManager &GetInstance(void);

  // Wait for the system to begin
  void StartStandbyMode();

  // Start recovery
  void StartRecoveryMode();

  // Check whether the frontend logger is in logging mode
  bool IsInLoggingMode();

  // Used to terminate current logging and wait for sleep mode
  void TerminateLoggingMode();

  // Used to wait for a certain mode (or not certain mode if is_equal is false)
  void WaitForMode(LoggingStatus logging_status, bool is_equal);

  // End the actual logging
  bool EndLogging();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Logging status associated with the front end logger of given type
  void SetLoggingStatus(LoggingStatus logging_status);

  LoggingStatus GetStatus();

  void ResetLoggingStatusMap();

  // Whether to enable or disable synchronous commit ?
  void SetSyncCommit(bool sync_commit) { syncronization_commit = sync_commit; }

  bool GetSyncCommit(void) const { return syncronization_commit; }

  size_t ActiveFrontendLoggerCount(void);

  BackendLogger *GetBackendLogger();

  bool RemoveBackendLogger(BackendLogger *backend_logger);

  void SetLogFileName(std::string log_file);

  std::string GetLogFileName(void);

  bool HasPelotonFrontendLogger() const {
    return (peloton_logging_mode == LOGGING_TYPE_NVM_NVM);
  }

  //===--------------------------------------------------------------------===//
  // Utility Functions
  //===--------------------------------------------------------------------===//

  FrontendLogger *GetFrontendLogger();

 private:
  LogManager();
  ~LogManager();

  bool RemoveFrontendLogger();

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // There is only one frontend_logger of a given type -- stdout, aries, peloton
  FrontendLogger *frontend_logger = nullptr;

  LoggingStatus logging_status = LOGGING_STATUS_TYPE_INVALID;

  // To synch the status map
  std::mutex logging_status_mutex;
  std::condition_variable logging_status_cv;

  bool syncronization_commit = false;

  std::string log_file_name;
};

}  // namespace logging
}  // namespace peloton
