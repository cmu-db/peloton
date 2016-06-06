//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.h
//
// Identification: src/include/logging/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>

#include "logging/logger.h"
#include "backend_logger.h"
#include "frontend_logger.h"
#include "concurrency/transaction.h"
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

#define LOG_FILE_LEN 1024 * UINT64_C(128)  // 128 MB

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

  // reset log status to invalid
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
    // TODO: Temporarily Disable logging
    return false;

    // Check the logging status
    return (logging_status == LOGGING_STATUS_TYPE_LOGGING);
  }

  // Used to terminate current logging and wait for sleep mode
  void TerminateLoggingMode();

  // Used to wait for a certain mode (or not certain mode if is_equal is false)
  void WaitForModeTransition(LoggingStatus logging_status, bool is_equal);

  // End the actual logging
  bool EndLogging();

  // method for frontend to inform waiting backends of a flush to disk
  void FrontendLoggerFlushed();

  // wait for the flush of a frontend logger (for worker thread)
  void WaitForFlush(cid_t cid);

  // get the current persistent flushed commit
  cid_t GetPersistentFlushedCommitId();

  // called by frontends when recovery is complete.(for a particular frontend)
  void NotifyRecoveryDone();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Logging status associated with the front end logger of given type
  void SetLoggingStatus(LoggingStatus logging_status);

  LoggingStatus GetLoggingStatus();

  // Whether to enable or disable synchronous commit ?
  void SetSyncCommit(bool sync_commit) { syncronization_commit = sync_commit; }

  // get the status of sychronus commit
  bool GetSyncCommit(void) const { return syncronization_commit; }

  // returns true if a frontend logger is active
  bool ContainsFrontendLogger(void);

  // get a backend logger
  // can give a hint of the frontend logger
  BackendLogger *GetBackendLogger(unsigned int hint = 0);

  // set the name of the log file (only used in wbl)
  void SetLogFileName(std::string log_file);

  // get the log file name (used only in wbl)
  std::string GetLogFileName(void);

  void SetLogDirectoryName(std::string log_dir);

  std::string GetLogDirectoryName(void);

  bool HasPelotonFrontendLogger() const {
    return (peloton_logging_mode == LOGGING_TYPE_NVM_WBL);
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

  // remove all frontend loggers (used for testing)
  void DropFrontendLoggers();

  // perpare to log must be called before a commit id is generated for a
  // transaction
  void PrepareLogging();

  // log the beginning of a commited transaction
  void LogBeginTransaction(cid_t commit_id);

  // log an update
  void LogUpdate(cid_t commit_id, const ItemPointer &old_version,
                 const ItemPointer &new_version);

  // log an insert
  void LogInsert(cid_t commit_id, const ItemPointer &new_location);

  // log a delete
  void LogDelete(cid_t commit_id, const ItemPointer &delete_location);

  // commit a transaction and wait until stable
  void LogCommitTransaction(cid_t commit_id);

  // used by the checkpointer to truncate unneeded log files
  void TruncateLogs(txn_id_t commit_id);

  // called if a transaction aborts before starting a commit
  void DoneLogging();

  // the maximum flushed commit id for all frontend loggers
  cid_t GetGlobalMaxFlushedIdForRecovery() {
    return global_max_flushed_id_for_recovery;
  }

  // set the max flushed id for recovery
  void SetGlobalMaxFlushedIdForRecovery(cid_t new_max) {
    global_max_flushed_id_for_recovery = new_max;
  }

  // updates the catalog and transaction managers to the correct oid and cid
  // after recovery
  void UpdateCatalogAndTxnManagers(oid_t new_oid, cid_t new_cid);

  // set the maximum commit id which has been persisted to disk
  void SetGlobalMaxFlushedCommitId(cid_t);

  // set the maximum commit id which has been persisted to disk
  cid_t GetGlobalMaxFlushedCommitId();

  // get the list of frontend loggers
  std::vector<std::unique_ptr<FrontendLogger>> &GetFrontendLoggersList() {
    return frontend_loggers;
  }

  // get the threshold for creating a new log file
  inline unsigned int GetLogFileSizeLimit() { return log_file_size_limit_; }

  // set the threshold for creating a new log file
  inline void SetLogFileSizeLimit(unsigned int file_size_limit) {
    log_file_size_limit_ = file_size_limit;
  }

  // get the beginning capacity of a log buffer
  inline unsigned int GetLogBufferCapacity() { return log_buffer_capacity_; }

  // set the initial capacity of log buffers passed between frontend and backend
  // loggers
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

  // test mode will not log to disk
  bool test_mode_ = false;

  // numbe of frontend loggers
  unsigned int num_frontend_loggers_ = DEFAULT_NUM_FRONTEND_LOGGERS;

  // set the strategy for mapping frontend loggers to worker threads
  LoggerMappingStrategyType logger_mapping_strategy_ = LOGGER_MAPPING_INVALID;

  // default log file size
  size_t log_file_size_limit_ = LOG_FILE_LEN;

  // default capacity for log buffer
  size_t log_buffer_capacity_ = LOG_FILE_LEN;

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

  // count of loggers who moved from the recovery to loggin state
  unsigned int recovery_to_logging_counter = 0;

  // maximum flushed commit id
  cid_t max_flushed_cid = 0;

  bool syncronization_commit =
      true;  // default should be true because it is safest

  // name of log file (for wbl)
  std::string log_file_name;

  std::string log_directory_name;

  // round robin counter for frontend logger assignment
  int frontend_logger_assign_counter;

  cid_t global_max_flushed_id_for_recovery = UINT64_MAX;

  cid_t global_max_flushed_commit_id = 0;

  // number the fronted loggers who have updated the manager of their max oid
  // and cid
  int update_managers_count = 0;

  // max oid after recovery
  oid_t max_oid = 0;

  // max cid after recovery
  cid_t max_cid = 0;
};

}  // namespace logging
}  // namespace peloton
