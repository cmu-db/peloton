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

#include "backend/concurrency/transaction_manager_factory.h"
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
#define NUM_FRONTEND_LOGGERS 2

// Each thread gets a backend logger
thread_local static BackendLogger *backend_logger = nullptr;
LoggingType LogManager::logging_type_ = LOGGING_TYPE_INVALID;
bool LogManager::test_mode_ = false;

LogManager::LogManager() { LogManager::Configure(peloton_logging_mode, false); }

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

  // TODO resolve this with eric
  // GetFrontendLogger();

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

  // before returning, query each frontend logger for its
  // max_delimiter_for_recovery, and choose the min of
  // all of them to be set as the global persistent log number
  // TODO handle corner case like no logs for a particular logger
  for (int i = 0; i < NUM_FRONTEND_LOGGERS; i++) {
    cid_t logger_max_flushed_id =
        frontend_loggers[i].get()->GetMaxDelimiterForRecovery();
    if (logger_max_flushed_id)
      global_max_flushed_id =
          std::min(global_max_flushed_id, logger_max_flushed_id);
  }
  LOG_INFO("Log manager set global_max_flushed_id as %d",
           (int)global_max_flushed_id);
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
    int frontend_logger_id = logger->GetFrontendLoggerID();
    LOG_INFO("Got frontend_logger_id as %d", (int)frontend_logger_id);
    frontend_loggers[frontend_logger_id]->SetBackendLoggerLoggedCid(*logger);
  }
}

void LogManager::DoneLogging() {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    logger->SetLoggingCidLowerBound(INVALID_CID);
  }
}

void LogManager::LogBeginTransaction(cid_t commit_id) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    TransactionRecord record(LOGRECORD_TYPE_TRANSACTION_BEGIN, commit_id);
    logger->Log(&record);
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
    // Can we avoid allocate tuple in head each time?
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    for (oid_t col = 0; col < schema->GetColumnCount(); col++) {
      tuple->SetValue(col,
                      new_tuple_tile_group->GetValue(new_version.offset, col),
                      executor_pool);
    }
    std::unique_ptr<LogRecord> record(
        logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, commit_id,
                               new_tuple_tile_group->GetTableId(),
                               new_tuple_tile_group->GetDatabaseId(),
                               new_version, old_version, tuple.get()));

    logger->Log(record.get());
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

    std::unique_ptr<LogRecord> record(logger->GetTupleRecord(
        LOGRECORD_TYPE_TUPLE_INSERT, commit_id, tile_group->GetTableId(),
        new_tuple_tile_group->GetDatabaseId(), new_location,
        INVALID_ITEMPOINTER, tuple.get()));
    logger->Log(record.get());
    delete executor_context;
  }
}

void LogManager::LogDelete(cid_t commit_id, ItemPointer &delete_location) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(delete_location.block);

    std::unique_ptr<LogRecord> record(logger->GetTupleRecord(
        LOGRECORD_TYPE_TUPLE_DELETE, commit_id, tile_group->GetTableId(),
        tile_group->GetDatabaseId(), INVALID_ITEMPOINTER, delete_location));

    logger->Log(record.get());
  }
}

void LogManager::LogCommitTransaction(cid_t commit_id) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    TransactionRecord record(LOGRECORD_TYPE_TRANSACTION_COMMIT, commit_id);
    logger->Log(&record);
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
    LOG_INFO("Creating a new backend logger!");
    backend_logger = BackendLogger::GetBackendLogger(logging_type_);
    int i;
    i = __sync_fetch_and_add(&this->frontend_logger_assign_counter, 1);
    frontend_loggers[i % NUM_FRONTEND_LOGGERS].get()->AddBackendLogger(
        backend_logger);
    backend_logger->SetFrontendLoggerID(i % NUM_FRONTEND_LOGGERS);
  }

  return backend_logger;
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the frontend logger otherwise nullptr
 */
FrontendLogger *LogManager::GetFrontendLogger() {
  /* int i = 0;
  return frontend_loggers[i].get(); */

  // If frontend logger doesn't exist
  /* if (frontend_logger == nullptr) {
    frontend_logger.reset(
        FrontendLogger::GetFrontendLogger(logging_type_, test_mode_));
  } */
  return std::unique_ptr<FrontendLogger>(FrontendLogger::GetFrontendLogger(
                                             logging_type_, test_mode_)).get();
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

void LogManager::PrepareRecovery() {
  if (prepared_recovery_) {
    return;
  }
  auto &catalog_manager = catalog::Manager::GetInstance();
  // for all database
  auto db_count = catalog_manager.GetDatabaseCount();
  for (oid_t db_idx = 0; db_idx < db_count; db_idx++) {
    auto database = catalog_manager.GetDatabase(db_idx);
    // for all tables
    auto table_count = database->GetTableCount();
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      auto table = database->GetTable(table_idx);
      // drop existing tile groups
      table->DropTileGroups();
    }
  }
  prepared_recovery_ = true;
}

void LogManager::DoneRecovery() {
  auto &catalog_manager = catalog::Manager::GetInstance();
  // for all database
  auto db_count = catalog_manager.GetDatabaseCount();
  for (oid_t db_idx = 0; db_idx < db_count; db_idx++) {
    auto database = catalog_manager.GetDatabase(db_idx);
    // for all tables
    auto table_count = database->GetTableCount();
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      auto table = database->GetTable(table_idx);
      if (table->GetTileGroupCount() == 0) {
        table->AddDefaultTileGroup();
      }
    }
  }
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
    LOG_INFO("FrontendLogger%d has max flushed commit id as %d", (int)i,
             (int)id);
    // assume 0 is INACTIVE STATE
    if (id && id < max_flushed_commit_id) max_flushed_commit_id = id;
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
  LOG_INFO("Waiting for flush with %d", (int)cid);
  {
    std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);

    // TODO confirm with mperron if this is correct or not
    while (this->GetMaxFlushedCommitId() < cid) {
      /* while (frontend_logger->GetMaxFlushedCommitId() < cid) { */
      LOG_INFO(
          "Logs up to %lu cid is flushed. %lu cid is not flushed yet. Wait...",
          this->GetMaxFlushedCommitId(), cid);
      flush_notify_cv.wait(wait_lock);
    }
    LOG_INFO("Flushes done! Can return! Got maxflush commit id as %d",
             (int)this->GetMaxFlushedCommitId());
  }
}

void LogManager::NotifyRecoveryDone() {
  LOG_INFO("One frontend logger has notified that it has completed recovery");

  int i = __sync_add_and_fetch(&this->recovery_to_logging_counter, 1);
  LOG_INFO("%d loggers have done recovery so far.",
           (int)recovery_to_logging_counter);
  if (i == NUM_FRONTEND_LOGGERS) {
    LOG_INFO("This was the last one! Change to LOGGING mode.");
    SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);
  }
}

void LogManager::UpdateCatalogAndTxnManagers(oid_t max_oid, cid_t max_cid) {
  {
    std::unique_lock<std::mutex> wait_lock(update_managers_mutex);
    auto &manager = catalog::Manager::GetInstance();
    if (max_oid > manager.GetNextOid()) {
      manager.SetNextOid(max_oid);
    }

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    if (txn_manager.GetNextCommitId() < max_cid) {
      txn_manager.SetNextCid(max_cid + 1);
    }
  }
}

}  // namespace logging
}  // namespace peloton
