//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.h
//
// Identification: src/backend/logging/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>

#include "backend/logging/logger.h"
#include "backend_logger.h"
#include "frontend_logger.h"
#include "backend/concurrency/transaction.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

extern LoggingType peloton_logging_mode;

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
  inline bool IsInLoggingMode() {
    // Check the logging status
    return (logging_status == LOGGING_STATUS_TYPE_LOGGING);
  }

  // Used to terminate current logging and wait for sleep mode
  void TerminateLoggingMode();

  // Used to wait for a certain mode (or not certain mode if is_equal is false)
  void WaitForModeTransition(LoggingStatus logging_status, bool is_equal);

  // End the actual logging
  bool EndLogging();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Logging status associated with the front end logger of given type
  void SetLoggingStatus(LoggingStatus logging_status);

  LoggingStatus GetLoggingStatus();

  // Whether to enable or disable synchronous commit ?
  void SetSyncCommit(bool sync_commit) { syncronization_commit = sync_commit; }

  bool GetSyncCommit(void) const { return syncronization_commit; }

  bool ContainsFrontendLogger(void);

  BackendLogger *GetBackendLogger();

  void SetLogFileName(std::string log_file);

  std::string GetLogFileName(void);

  bool HasPelotonFrontendLogger() const {
    return (peloton_logging_mode == LOGGING_TYPE_NVM_NVM);
  }

  //===--------------------------------------------------------------------===//
  // Utility Functions
  //===--------------------------------------------------------------------===//

  FrontendLogger *GetFrontendLogger();

  void ResetFrontendLogger();

  void LogBeginTransaction(oid_t commit_id);

  void LogUpdate(concurrency::Transaction *curr_txn, cid_t commit_id,
                 ItemPointer &old_version, ItemPointer &new_version);

  void LogInsert(concurrency::Transaction *curr_txn, cid_t commit_id,
                 ItemPointer &new_location);

  void LogDelete(oid_t commit_id, ItemPointer &delete_location);

  void LogCommitTransaction(oid_t commit_id);

 private:
  LogManager();
  ~LogManager();

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // There is only one frontend_logger of some type
  // either write ahead or write behind logging
  std::unique_ptr<FrontendLogger> frontend_logger;

  LoggingStatus logging_status = LOGGING_STATUS_TYPE_INVALID;

  // To synch the status
  std::mutex logging_status_mutex;
  std::condition_variable logging_status_cv;

  bool syncronization_commit = false;

  std::string log_file_name;
};

}  // namespace logging
}  // namespace peloton
