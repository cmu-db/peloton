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
#include "loggers/wal_frontend_logger.h"

#define DEFAULT_NUM_FRONTEND_LOGGERS 1

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

  // configuration
  void Configure(LoggingType logging_type, bool test_mode = false,
                 unsigned int num_frontend_loggers = 1,
                 LoggerMappingStrategyType logger_mapping_strategy =
                     LOGGER_MAPPING_ROUND_ROBIN) {
    logging_type_ = logging_type;
    test_mode_ = test_mode;
    num_frontend_loggers_ = num_frontend_loggers;
    logger_mapping_strategy_ = logger_mapping_strategy;
  }

  // reset all frontend loggers, for testing
  void ResetFrontendLoggers() {
    for (auto &frontend_logger : frontend_loggers) {
      frontend_logger->Reset();
    }
  }

  void ResetLogStatus() {
    this->recovery_to_logging_counter = 0;
    SetLoggingStatus(LOGGING_STATUS_TYPE_INVALID);
  }

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

  void FrontendLoggerFlushed();

  void WaitForFlush(cid_t cid);

  cid_t GetPersistentFlushedCommitId();

  void NotifyRecoveryDone();

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

  BackendLogger *GetBackendLogger(unsigned int hint = 0);

  void SetLogFileName(std::string log_file);

  std::string GetLogFileName(void);

  bool HasPelotonFrontendLogger() const {
    return (peloton_logging_mode == LOGGING_TYPE_NVM_WAL);
  }

  // Drop all default tiles for tables before recovery
  void PrepareRecovery();

  // Add default tiles for tables if necessary
  void DoneRecovery();

  //===--------------------------------------------------------------------===//
  // Utility Functions
  //===--------------------------------------------------------------------===//

  // initialize a list of frontend loggers
  void InitFrontendLoggers();

  // get a frontend logger at given index
  FrontendLogger *GetFrontendLogger(unsigned int logger_idx);

  void ResetFrontendLogger();

  void DropFrontendLoggers();

  void PrepareLogging();

  void LogBeginTransaction(cid_t commit_id);

  void LogUpdate(cid_t commit_id, ItemPointer &old_version,
                 ItemPointer &new_version);

  void LogInsert(cid_t commit_id, ItemPointer &new_location);

  void LogDelete(cid_t commit_id, ItemPointer &delete_location);

  void LogCommitTransaction(cid_t commit_id);

  void TruncateLogs(txn_id_t commit_id);

  void DoneLogging();

  cid_t GetGlobalMaxFlushedIdForRecovery() {
    return global_max_flushed_id_for_recovery;
  }

  void SetGlobalMaxFlushedIdForRecovery(cid_t new_max) {
    global_max_flushed_id_for_recovery = new_max;
  }
  void UpdateCatalogAndTxnManagers(oid_t, cid_t);

  void SetGlobalMaxFlushedCommitId(cid_t);

  cid_t GetGlobalMaxFlushedCommitId();

  std::vector<std::unique_ptr<FrontendLogger>> &GetFrontendLoggersList() {
    return frontend_loggers;
  }

  inline unsigned int GetLogFileSizeLimit() { return log_file_size_limit_; }

  inline void SetLogFileSizeLimit(unsigned int file_size_limit) {
    log_file_size_limit_ = file_size_limit;
  }

  inline unsigned int GetLogBufferCapacity() { return log_buffer_capacity_; }

  inline void SetLogBufferCapacity(unsigned int log_buffer_capacity) {
    log_buffer_capacity_ = log_buffer_capacity;
  }

 private:
  LogManager();
  ~LogManager();

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // static configurations for logging
  LoggingType logging_type_ = LOGGING_TYPE_INVALID;

  bool test_mode_ = false;

  unsigned int num_frontend_loggers_ = DEFAULT_NUM_FRONTEND_LOGGERS;

  LoggerMappingStrategyType logger_mapping_strategy_ = LOGGER_MAPPING_INVALID;

  // default log file size: 32 MB
  unsigned int log_file_size_limit_ = 32768;

  // default capacity for log buffer
  unsigned int log_buffer_capacity_ = 32768;

  // There is only one frontend_logger of some type
  // either write ahead or write behind logging
  std::vector<std::unique_ptr<FrontendLogger>> frontend_loggers;

  LoggingStatus logging_status = LOGGING_STATUS_TYPE_INVALID;

  bool prepared_recovery_ = false;

  // To synch the status
  std::mutex logging_status_mutex;
  std::condition_variable logging_status_cv;

  // To wait for flush
  std::mutex flush_notify_mutex;
  std::condition_variable flush_notify_cv;

  // To update catalog and txn managers
  std::mutex update_managers_mutex;

  unsigned int recovery_to_logging_counter = 0;

  cid_t max_flushed_cid = 0;

  bool syncronization_commit =
      true;  // default should be true because it is safest

  std::string log_file_name;

  int frontend_logger_assign_counter;

  cid_t global_max_flushed_id_for_recovery = UINT64_MAX;

  cid_t global_max_flushed_commit_id = 0;

  int update_managers_count = 0;

  oid_t max_oid = 0;

  cid_t max_cid = 0;
};

}  // namespace logging
}  // namespace peloton
