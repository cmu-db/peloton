//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <condition_variable>
#include <memory>

#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/common/logger.h"
#include "backend/executor/executor_context.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace logging {

#define LOG_FILE_NAME "wal.log"
#define NUM_FRONTEND_LOGGERS 16

// Each thread gets a backend logger
thread_local static BackendLogger *backend_logger = nullptr;

LogManager::LogManager() { this->frontend_logger_assign_counter = 0; }

LogManager::~LogManager() {}

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
  LOG_INFO("TRACKING: LogManager::StartStandbyMode()");
  if (frontend_loggers.size() == 0) {
    for (int i = 0; i < NUM_FRONTEND_LOGGERS; i++) {
      std::unique_ptr<FrontendLogger> frontend_logger(
          FrontendLogger::GetFrontendLogger(peloton_logging_mode));

      if (frontend_logger.get() != nullptr) {
        // frontend_logger.get()->SetLoggerID(i);
        frontend_loggers.push_back(std::move(frontend_logger));
      }
    }
  }

  // If frontend logger still doesn't exist, then we have disabled logging
  if (frontend_loggers.size() == 0) {
    LOG_INFO("We have disabled logging");
    return;
  }

  // Toggle status in log manager map
  SetLoggingStatus(LOGGING_STATUS_TYPE_STANDBY);

  // Launch the frontend logger's main loop
  for (int i = 0; i < NUM_FRONTEND_LOGGERS; i++) {
    std::thread(&FrontendLogger::MainLoop, frontend_loggers[i].get()).detach();
    // frontend_loggers[i].get()->MainLoop();
  }
}

void LogManager::StartRecoveryMode() {
  // Toggle the status after STANDBY
  LOG_INFO("TRACKING: LogManager::StartRecoveryMode()");
  SetLoggingStatus(LOGGING_STATUS_TYPE_RECOVERY);
}

void LogManager::TerminateLoggingMode() {
  LOG_INFO("TRACKING: LogManager::TerminateLoggingMode()");
  SetLoggingStatus(LOGGING_STATUS_TYPE_TERMINATE);

  // We set the frontend logger status to Terminate
  // And, then we wait for the transition to sleep mode
  WaitForModeTransition(LOGGING_STATUS_TYPE_SLEEP, true);
}

void LogManager::WaitForModeTransition(LoggingStatus logging_status_,
                                       bool is_equal) {
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
  LOG_INFO("TRACKING: LogManager::EndLogging()");
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

void LogManager::PrepareLogging() {
  if (this->IsInLoggingMode()) {
    this->GetBackendLogger();
    auto logger = this->GetBackendLogger();
    frontend_logger->SetBackendLoggerLoggedCid(*logger);
  }
}

void LogManager::DoneLogging() {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    logger->SetHighestLoggedCommitId(INVALID_CID);
  }
}

void LogManager::LogBeginTransaction(cid_t commit_id) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto record =
        new TransactionRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN, commit_id);
    logger->Log(record);
  }
}

void LogManager::LogUpdate(concurrency::Transaction *curr_txn, cid_t commit_id,
                           ItemPointer &old_version, ItemPointer &new_version) {
  if (this->IsInLoggingMode()) {
    auto executor_context = new executor::ExecutorContext(curr_txn);
    auto executor_pool = executor_context->GetExecutorContextPool();
    auto &manager = catalog::Manager::GetInstance();

    auto new_tuple_tile_group = manager.GetTileGroup(new_version.block);

    auto logger = this->GetBackendLogger();
    auto schema = manager.GetTableWithOid(new_tuple_tile_group->GetDatabaseId(),
                                          new_tuple_tile_group->GetTableId())
                      ->GetSchema();

    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    for (oid_t col = 0; col < schema->GetColumnCount(); col++) {
      tuple->SetValue(col,
                      new_tuple_tile_group->GetValue(new_version.offset, col),
                      executor_pool);
    }
    auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, commit_id,
                                         new_tuple_tile_group->GetTableId(),
                                         new_tuple_tile_group->GetDatabaseId(),
                                         new_version, old_version, tuple.get());

    logger->Log(record);
    delete executor_context;
  }
}

void LogManager::LogInsert(concurrency::Transaction *curr_txn, cid_t commit_id,
                           ItemPointer &new_location) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto &manager = catalog::Manager::GetInstance();

    auto new_tuple_tile_group = manager.GetTileGroup(new_location.block);
    auto executor_context = new executor::ExecutorContext(curr_txn);
    auto executor_pool = executor_context->GetExecutorContextPool();

    auto tile_group = manager.GetTileGroup(new_location.block);
    auto schema =
        manager.GetTableWithOid(tile_group->GetDatabaseId(),
                                tile_group->GetTableId())->GetSchema();
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    for (oid_t col = 0; col < schema->GetColumnCount(); col++) {
      tuple->SetValue(col,
                      new_tuple_tile_group->GetValue(new_location.offset, col),
                      executor_pool);
    }

    auto record = logger->GetTupleRecord(
        LOGRECORD_TYPE_TUPLE_INSERT, commit_id, tile_group->GetTableId(),
        new_tuple_tile_group->GetDatabaseId(), new_location,
        INVALID_ITEMPOINTER, tuple.get());
    logger->Log(record);
    delete executor_context;
  }
}

void LogManager::LogDelete(cid_t commit_id, ItemPointer &delete_location) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(delete_location.block);

    auto record = logger->GetTupleRecord(
        LOGRECORD_TYPE_TUPLE_DELETE, commit_id, tile_group->GetTableId(),
        tile_group->GetDatabaseId(), INVALID_ITEMPOINTER, delete_location);

    logger->Log(record);
  }
}

void LogManager::LogCommitTransaction(cid_t commit_id) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto record =
        new TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, commit_id);
    logger->Log(record);
    WaitForFlush(commit_id);
  }
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *LogManager::GetBackendLogger() {
  assert(frontend_loggers.size() != 0);

  // Check whether the backend logger exists or not
  // if not, create a backend logger and store it in frontend logger
  if (backend_logger == nullptr) {
    backend_logger = BackendLogger::GetBackendLogger(peloton_logging_mode);
    // TODO for now, add all backend loggers to FrontendLogger(0), change this
    int i;
    i = __sync_fetch_and_add(&this->frontend_logger_assign_counter, 1);
    frontend_loggers[i % NUM_FRONTEND_LOGGERS].get()->AddBackendLogger(
        backend_logger);
  }

  return backend_logger;
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the frontend logger otherwise nullptr
 */
FrontendLogger *LogManager::GetFrontendLogger() {
  // TODO return appropriate frontend logger, currently return the 0th one
  int i = 0;
  return frontend_loggers[i].get();
}

bool LogManager::ContainsFrontendLogger(void) {
  return (frontend_loggers.size() != 0);
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

void LogManager::ResetFrontendLogger() {
  // TODO don't know what to do here, so just reset the first one among them
  int i = 0;
  frontend_loggers[i].reset(
      FrontendLogger::GetFrontendLogger(peloton_logging_mode));
}

void LogManager::TruncateLogs(txn_id_t commit_id) {
  int num_loggers;

  num_loggers = this->frontend_loggers.size();

  for (int i = 0; i < num_loggers; i++) {
    FrontendLogger *frontend_logger = this->frontend_loggers[i].get();
    reinterpret_cast<WriteAheadFrontendLogger *>(frontend_logger)
        ->TruncateLog(commit_id);
  }
}

cid_t LogManager::GetMaxFlushedCommitId() {

  int num_loggers;
  num_loggers = this->frontend_loggers.size();
  cid_t max_flushed_commit_id = UINT64_MAX, id;

  // TODO confirm with mperron if this is correct or not
  for (int i = 0; i < num_loggers; i++) {
    FrontendLogger *frontend_logger = this->frontend_loggers[i].get();
    id = reinterpret_cast<WriteAheadFrontendLogger *>(frontend_logger)
        ->GetMaxFlushedCommitId();
    if (id < max_flushed_commit_id)
      max_flushed_commit_id = id;
  }

  return max_flushed_commit_id;
}

void LogManager::FrontendLoggerFlushed() {
  {
    std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);
    flush_notify_cv.notify_all();
    // LOG_INFO("FrontendLoggerFlushed - notify all waiting backend loggers");
  }
}

void LogManager::WaitForFlush(cid_t cid) {
  {
    std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);

    // TODO confirm with mperron if this is correct or not
    while (this->GetMaxFlushedCommitId() < cid) {
    // while (frontend_logger->GetMaxFlushedCommitId() < cid) {
      LOG_INFO("Logs up to %lu cid is flushed. %lu cid is not flushed yet. Wait...",
    		  frontend_logger->GetMaxFlushedCommitId(), cid);
      flush_notify_cv.wait(wait_lock);
    }
  }
}

}  // namespace logging
}  // namespace peloton
