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

#include <condition_variable>
#include <memory>

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/common/logger.h"
#include "backend/common/macros.h"
#include "backend/executor/executor_context.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/database.h"

namespace peloton {
namespace logging {

// Each thread gets a backend logger
thread_local static BackendLogger *backend_logger = nullptr;

LogManager::LogManager() {
  Configure(peloton_logging_mode, false, DEFAULT_NUM_FRONTEND_LOGGERS,
            LOGGER_MAPPING_ROUND_ROBIN);
}

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
  LOG_TRACE("TRACKING: LogManager::StartStandbyMode()");
  InitFrontendLoggers();

  // If frontend logger still doesn't exist, then we have disabled logging
  if (frontend_loggers.size() == 0) {
    LOG_TRACE("We have disabled logging");
    return;
  }

  // assign a distinguished logger thread only if we have more than 1 loggers
  if (num_frontend_loggers_ > 1)
    frontend_loggers[0].get()->SetIsDistinguishedLogger(true);

  // Toggle status in log manager map
  SetLoggingStatus(LOGGING_STATUS_TYPE_STANDBY);

  // Launch the frontend logger's main loop
  for (unsigned int i = 0; i < num_frontend_loggers_; i++) {
    std::thread(&FrontendLogger::MainLoop, frontend_loggers[i].get()).detach();
  }

  // before returning, query each frontend logger for its
  // max_delimiter_for_recovery, and choose the min of
  // all of them to be set as the global persistent log number
  // TODO handle corner case like no logs for a particular logger
  for (unsigned int i = 0; i < num_frontend_loggers_; i++) {
    cid_t logger_max_flushed_id =
        frontend_loggers[i].get()->GetMaxDelimiterForRecovery();
    if (logger_max_flushed_id)
      global_max_flushed_id_for_recovery =
          std::min(global_max_flushed_id_for_recovery, logger_max_flushed_id);
  }
  if (global_max_flushed_id_for_recovery ==
      UINT64_MAX)  // no one has logged anything
    global_max_flushed_id_for_recovery = 0;
  LOG_TRACE("Log manager set global_max_flushed_id_for_recovery as %d",
           (int)global_max_flushed_id_for_recovery);
}

void LogManager::StartRecoveryMode() {
  // Toggle the status after STANDBY
  LOG_TRACE("TRACKING: LogManager::StartRecoveryMode()");
  SetLoggingStatus(LOGGING_STATUS_TYPE_RECOVERY);
}

void LogManager::TerminateLoggingMode() {
  LOG_TRACE("TRACKING: LogManager::TerminateLoggingMode()");
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
  LOG_TRACE("TRACKING: LogManager::EndLogging()");
  WaitForModeTransition(LOGGING_STATUS_TYPE_RECOVERY, false);

  LOG_TRACE("Wait until frontend logger escapes main loop..");

  // Wait for the frontend logger to enter sleep mode
  TerminateLoggingMode();

  LOG_TRACE("Escaped from MainLoop");

  // Remove the frontend logger
  SetLoggingStatus(LOGGING_STATUS_TYPE_INVALID);
  LOG_TRACE("Terminated successfully");

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
    LOG_TRACE("Got frontend_logger_id as %d", (int)frontend_logger_id);
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

void LogManager::LogUpdate(cid_t commit_id, const ItemPointer &old_version,
		const ItemPointer &new_version) {
  if (this->IsInLoggingMode()) {
    auto &manager = catalog::Manager::GetInstance();

    auto new_tuple_tile_group = manager.GetTileGroup(new_version.block);

    auto logger = this->GetBackendLogger();

    std::unique_ptr<LogRecord> record;
    std::unique_ptr<storage::Tuple> tuple;
    auto schema = manager.GetTableWithOid(new_tuple_tile_group->GetDatabaseId(),
                                          new_tuple_tile_group->GetTableId())
                      ->GetSchema();
    // Can we avoid allocate tuple in head each time?
    if (IsBasedOnWriteAheadLogging(logging_type_) || replicating_){
    tuple.reset(new storage::Tuple(schema, true));
    for (oid_t col = 0; col < schema->GetColumnCount(); col++) {
      tuple->SetValue(col,
                      new_tuple_tile_group->GetValue(new_version.offset, col),
                      logger->GetVarlenPool());
    }
    record.reset(
        logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, commit_id,
                               new_tuple_tile_group->GetTableId(),
                               new_tuple_tile_group->GetDatabaseId(),
                               new_version, old_version, tuple.get()));
    }else{
    	// if wbl without replication, do not include tuple data
    	record.reset(
    	        logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, commit_id,
    	                               new_tuple_tile_group->GetTableId(),
    	                               new_tuple_tile_group->GetDatabaseId(),
    	                               new_version, old_version));
    }

    logger->Log(record.get());
  }
}

void LogManager::LogInsert(cid_t commit_id, const ItemPointer &new_location) {
  if (this->IsInLoggingMode()) {
    auto logger = this->GetBackendLogger();
    auto &manager = catalog::Manager::GetInstance();

    auto new_tuple_tile_group = manager.GetTileGroup(new_location.block);

    auto tile_group = manager.GetTileGroup(new_location.block);
    std::unique_ptr<LogRecord> record;
    std::unique_ptr<storage::Tuple> tuple;
    if (IsBasedOnWriteAheadLogging(logging_type_) || replicating_){
		auto schema =
			manager.GetTableWithOid(tile_group->GetDatabaseId(),
									tile_group->GetTableId())->GetSchema();
		tuple.reset(new storage::Tuple(schema, true));
		for (oid_t col = 0; col < schema->GetColumnCount(); col++) {
		  tuple->SetValue(col,
						  new_tuple_tile_group->GetValue(new_location.offset, col),
						  logger->GetVarlenPool());
		}

		record.reset(logger->GetTupleRecord(
			LOGRECORD_TYPE_TUPLE_INSERT, commit_id, tile_group->GetTableId(),
			new_tuple_tile_group->GetDatabaseId(), new_location,
			INVALID_ITEMPOINTER, tuple.get()));

    }else{
    	// do not construct the tuple for the wbl case
    	record.reset(logger->GetTupleRecord(
    				LOGRECORD_TYPE_TUPLE_INSERT, commit_id, tile_group->GetTableId(),
    				new_tuple_tile_group->GetDatabaseId(), new_location,
    				INVALID_ITEMPOINTER));
    }
    logger->Log(record.get());

  }
}

void LogManager::LogDelete(cid_t commit_id, const ItemPointer &delete_location) {
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
    if (syncronization_commit) {
      WaitForFlush(commit_id);
    }
    logger->GetVarlenPool()->Purge();
  }
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *LogManager::GetBackendLogger(unsigned int hint_idx) {
  PL_ASSERT(frontend_loggers.size() != 0);

  // Check whether the backend logger exists or not
  // if not, create a backend logger and store it in frontend logger
  if (backend_logger == nullptr) {
    PL_ASSERT(logger_mapping_strategy_ != LOGGER_MAPPING_INVALID);
    LOG_TRACE("Creating a new backend logger!");
    backend_logger = BackendLogger::GetBackendLogger(logging_type_);
    int i = __sync_fetch_and_add(&this->frontend_logger_assign_counter, 1);

    // round robin mapping
    if (logger_mapping_strategy_ == LOGGER_MAPPING_ROUND_ROBIN) {
      frontend_loggers[i % num_frontend_loggers_].get()->AddBackendLogger(
          backend_logger);
      backend_logger->SetFrontendLoggerID(i % num_frontend_loggers_);

    } else if (logger_mapping_strategy_ == LOGGER_MAPPING_MANUAL) {
      // manual mapping with hint
      PL_ASSERT(hint_idx < frontend_loggers.size());
      frontend_loggers[hint_idx].get()->AddBackendLogger(backend_logger);
      backend_logger->SetFrontendLoggerID(hint_idx);
    } else {
      LOG_ERROR("Unsupported Logger Mapping Strategy");
      PL_ASSERT(false);
      return nullptr;
    }
  }

  return backend_logger;
}

void LogManager::InitFrontendLoggers() {
  if (frontend_loggers.size() == 0) {
    for (unsigned int i = 0; i < num_frontend_loggers_; i++) {
      std::unique_ptr<FrontendLogger> frontend_logger(
          FrontendLogger::GetFrontendLogger(logging_type_, test_mode_));

      if (frontend_logger.get() != nullptr) {
        frontend_loggers.push_back(std::move(frontend_logger));
      }
    }
  }
}

/**
 * @brief Find Frontend Logger in Log Manager
 * @param logging type can be stdout(debug), aries, peloton
 * @return the frontend logger otherwise nullptr
 */
FrontendLogger *LogManager::GetFrontendLogger(unsigned int logger_idx) {
  PL_ASSERT(logger_idx < frontend_loggers.size());
  return frontend_loggers[logger_idx].get();
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
  PL_ASSERT(log_file_name.empty() == false);
  return log_file_name;
}

void LogManager::SetLogDirectoryName(std::string log_directory) {
  log_directory_name = log_directory;
}

// XXX change to read configuration file
std::string LogManager::GetLogDirectoryName(void) {
  return log_directory_name;
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
      table->SetNumberOfTuples(0);
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

void LogManager::DropFrontendLoggers() {
  ResetFrontendLoggers();
  frontend_loggers.clear();
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

cid_t LogManager::GetGlobalMaxFlushedCommitId() {
  return global_max_flushed_commit_id;
}

void LogManager::SetGlobalMaxFlushedCommitId(cid_t new_max) {
  if (new_max != global_max_flushed_commit_id) {
    LOG_TRACE("Setting global_max_flushed_commit_id to %d", (int)new_max);
  }
  global_max_flushed_commit_id = new_max;
}

cid_t LogManager::GetPersistentFlushedCommitId() {
  int num_loggers;
  num_loggers = this->frontend_loggers.size();
  cid_t persistent_flushed_commit_id = UINT64_MAX, id;

  for (int i = 0; i < num_loggers; i++) {
    FrontendLogger *frontend_logger = this->frontend_loggers[i].get();
    id = reinterpret_cast<WriteAheadFrontendLogger *>(frontend_logger)
             ->GetMaxFlushedCommitId();
    LOG_TRACE("FrontendLogger%d has max flushed commit id as %d", (int)i,
             (int)id);

    // assume 0 is INACTIVE STATE
    if (id != INVALID_CID && id < persistent_flushed_commit_id)
      persistent_flushed_commit_id = id;
  }

  if (persistent_flushed_commit_id == UINT64_MAX)  // no one is doing anything
    return INVALID_CID;

  return persistent_flushed_commit_id;
}

void LogManager::FrontendLoggerFlushed() {
  {
    std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);
    flush_notify_cv.notify_all();
  }
}

void LogManager::WaitForFlush(cid_t cid) {
  LOG_TRACE("Waiting for flush with %d", (int)cid);
  {
    std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);

    while (this->GetPersistentFlushedCommitId() < cid) {
      LOG_TRACE(
          "Logs up to %lu cid is flushed. %lu cid is not flushed yet. Wait...",
          this->GetPersistentFlushedCommitId(), cid);
      flush_notify_cv.wait(wait_lock);
    }
    LOG_TRACE("Flushes done! Can return! Got persistent flushed commit id as %d",
             (int)this->GetPersistentFlushedCommitId());
  }
}

void LogManager::NotifyRecoveryDone() {
  LOG_TRACE("One frontend logger has notified that it has completed recovery");

  unsigned int i = __sync_add_and_fetch(&this->recovery_to_logging_counter, 1);
  LOG_TRACE("%d loggers have done recovery so far.",
           (int)recovery_to_logging_counter);
  if (i == num_frontend_loggers_) {
    LOG_TRACE(
        "This was the last one! Recover Index and change to LOGGING mode.");
    frontend_loggers[0].get()->RecoverIndex();
    SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);
  }
}

void LogManager::UpdateCatalogAndTxnManagers(oid_t new_oid, cid_t new_cid) {
  {
    std::unique_lock<std::mutex> wait_lock(update_managers_mutex);

    update_managers_count++;

    max_oid = std::max(max_oid, new_oid);

    max_cid = std::max(max_cid, new_cid);

    if (update_managers_count == (int)num_frontend_loggers_) {
      auto &manager = catalog::Manager::GetInstance();
      manager.SetNextOid(max_oid);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      txn_manager.SetNextCid(max_cid + 1);
    }
  }
}

}  // namespace logging
}  // namespace peloton
