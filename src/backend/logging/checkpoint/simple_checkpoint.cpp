/*-------------------------------------------------------------------------
 *
 * simple_checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint/simple_checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/log_record.h"
#include "backend/logging/checkpoint_tile_scanner.h"
#include "backend/logging/logging_util.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/executors.h"
#include "backend/executor/executor_context.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/catalog/manager.h"

#include "backend/common/logger.h"
#include "backend/common/types.h"

#include "backend/logging/log_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/checkpoint_manager.h"
#include "backend/storage/database.h"
#include "backend/executor/seq_scan_executor.h"

namespace peloton {
namespace logging {
//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

SimpleCheckpoint::SimpleCheckpoint(bool disable_file_access)
    : Checkpoint(disable_file_access), logger_(nullptr) {
  InitDirectory();
  InitVersionNumber();
}

SimpleCheckpoint::~SimpleCheckpoint() {
  for (auto &record : records_) {
    record.reset();
  }
  records_.clear();
}

void SimpleCheckpoint::DoCheckpoint() {
  // TODO split checkpoint file into multiple files in the future
  // Create a new file for checkpoint
  CreateFile();

  auto &log_manager = LogManager::GetInstance();
  if (logger_ == nullptr) {
    logger_.reset(BackendLogger::GetBackendLogger(LOGGING_TYPE_NVM_WAL));
  }

  start_commit_id_ = log_manager.GetGlobalMaxFlushedCommitId();
  if (start_commit_id_ == INVALID_CID) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    start_commit_id_ = txn_manager.GetMaxCommittedCid();
  }

  LOG_TRACE("DoCheckpoint cid = %lu", start_commit_id_);

  // Add txn begin record
  std::shared_ptr<LogRecord> begin_record(new TransactionRecord(
      LOGRECORD_TYPE_TRANSACTION_BEGIN, start_commit_id_));
  CopySerializeOutput begin_output_buffer;
  begin_record->Serialize(begin_output_buffer);
  records_.push_back(begin_record);

  auto &catalog_manager = catalog::Manager::GetInstance();
  auto database_count = catalog_manager.GetDatabaseCount();

  // loop all databases
  for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
    auto database = catalog_manager.GetDatabase(database_idx);
    auto table_count = database->GetTableCount();
    auto database_oid = database->GetOid();

    // loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *target_table = database->GetTable(table_idx);
      PL_ASSERT(target_table);
      LOG_TRACE("SeqScan: database idx %u table idx %u: %s", database_idx,
               table_idx, target_table->GetName().c_str());
      Scan(target_table, database_oid);
    }
  }

  // Add txn commit record
  std::shared_ptr<LogRecord> commit_record(new TransactionRecord(
      LOGRECORD_TYPE_TRANSACTION_COMMIT, start_commit_id_));
  CopySerializeOutput commit_output_buffer;
  commit_record->Serialize(commit_output_buffer);
  records_.push_back(commit_record);

  // TODO Add delimiter record for checkpoint recovery as well
  Persist();

  Cleanup();
  most_recent_checkpoint_cid = start_commit_id_;
}

cid_t SimpleCheckpoint::DoRecovery() {
  // No checkpoint to recover from
  if (checkpoint_version < 0) {
    return 0;
  }
  // we open checkpoint file in read + binary mode
  std::string file_name = ConcatFileName(checkpoint_dir, checkpoint_version);
  bool success =
      LoggingUtil::InitFileHandle(file_name.c_str(), file_handle_, "rb");
  if (!success) {
    return 0;
  }

  auto size = LoggingUtil::GetLogFileSize(file_handle_);
  PL_ASSERT(size > 0);
  file_handle_.size = size;

  bool should_stop = false;
  cid_t commit_id = 0;
  while (!should_stop) {
    auto record_type = LoggingUtil::GetNextLogRecordType(file_handle_);
    switch (record_type) {
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT: {
        LOG_TRACE("Read checkpoint insert entry");
        InsertTuple(commit_id);
        break;
      }
      case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
        should_stop = true;
        break;
      }
      case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
        LOG_TRACE("Read checkpoint begin entry");
        TransactionRecord txn_rec(record_type);
        if (LoggingUtil::ReadTransactionRecordHeader(txn_rec, file_handle_) ==
            false) {
          LOG_ERROR("Failed to read checkpoint begin entry");
          return false;
        }
        commit_id = txn_rec.GetTransactionId();
        break;
      }
      default: {
        LOG_ERROR("Invalid checkpoint entry");
        should_stop = true;
        break;
      }
    }
  }

  // After finishing recovery, set the next oid with maximum oid
  // observed during the recovery
  auto &manager = catalog::Manager::GetInstance();
  if (max_oid_ > manager.GetNextOid()) {
    manager.SetNextOid(max_oid_);
  }

  // FIXME this is not thread safe for concurrent checkpoint recovery
  concurrency::TransactionManagerFactory::GetInstance().SetNextCid(commit_id);
  CheckpointManager::GetInstance().SetRecoveredCid(commit_id);
  return commit_id;
}

void SimpleCheckpoint::InsertTuple(cid_t commit_id) {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  // Check for torn log write
  if (LoggingUtil::ReadTupleRecordHeader(tuple_record, file_handle_) == false) {
    LOG_ERROR("Could not read tuple record header.");
    return;
  }

  auto table = LoggingUtil::GetTable(tuple_record);
  if (!table) {
    // the table was deleted
    LoggingUtil::SkipTupleRecordBody(file_handle_);
    return;
  }

  // Read off the tuple record body from the log
  std::unique_ptr<storage::Tuple> tuple(LoggingUtil::ReadTupleRecordBody(
      table->GetSchema(), pool.get(), file_handle_));
  // Check for torn log write
  if (tuple == nullptr) {
    LOG_ERROR("Torn checkpoint write.");
    return;
  }
  auto target_location = tuple_record.GetInsertLocation();
  auto tile_group_id = target_location.block;
  RecoverTuple(tuple.get(), table, target_location, commit_id);
  if (max_oid_ < target_location.block) {
    max_oid_ = tile_group_id;
  }
  LOG_TRACE("Inserted a tuple from checkpoint: (%u, %u)",
            target_location.block, target_location.offset);
}

void SimpleCheckpoint::Scan(storage::DataTable *target_table,
                            oid_t database_oid) {
  auto schema = target_table->GetSchema();
  PL_ASSERT(schema);
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  oid_t current_tile_group_offset = START_OID;
  auto table_tile_group_count = target_table->GetTileGroupCount();
  CheckpointTileScanner scanner;

  // TODO scan assigned tile in multi-thread checkpoint
  while (current_tile_group_offset < table_tile_group_count) {
    // Retrieve a tile group
    auto tile_group = target_table->GetTileGroup(current_tile_group_offset);

    // Retrieve a logical tile
    std::unique_ptr<executor::LogicalTile> logical_tile(
        scanner.Scan(tile_group, column_ids, start_commit_id_));

    // Empty result
    if (!logical_tile) {
      current_tile_group_offset++;
      continue;
    }

    auto tile_group_id = logical_tile->GetColumnInfo(0)
                             .base_tile->GetTileGroup()
                             ->GetTileGroupId();

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          logical_tile.get(), tuple_id);

      // Logging
      {
        // construct a physical tuple from the logical tuple
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        for (auto column_id : column_ids) {
          tuple->SetValue(column_id, cur_tuple.GetValue(column_id),
                          this->pool.get());
        }
        ItemPointer location(tile_group_id, tuple_id);
        // TODO is it possible to avoid `new` for checkpoint?
        std::shared_ptr<LogRecord> record(logger_->GetTupleRecord(
            LOGRECORD_TYPE_TUPLE_INSERT, INITIAL_TXN_ID, target_table->GetOid(),
            database_oid, location, INVALID_ITEMPOINTER, tuple.get()));
        PL_ASSERT(record);
        CopySerializeOutput output_buffer;
        record->Serialize(output_buffer);
        LOG_TRACE("Insert a new record for checkpoint (%u, %u)", tile_group_id,
                  tuple_id);
        records_.push_back(record);
      }
    }
    // persist to file once per tile
    Persist();
    current_tile_group_offset++;
  }
}

void SimpleCheckpoint::SetLogger(BackendLogger *logger) {
  logger_.reset(logger);
}

std::vector<std::shared_ptr<LogRecord>> SimpleCheckpoint::GetRecords() {
  return records_;
}

// Private Functions
void SimpleCheckpoint::CreateFile() {
  if (disable_file_access) return;
  // open checkpoint file and file descriptor
  std::string file_name = ConcatFileName(checkpoint_dir, ++checkpoint_version);
  bool success =
      LoggingUtil::InitFileHandle(file_name.c_str(), file_handle_, "ab");
  if (!success) {
    PL_ASSERT(false);
    return;
  }
  LOG_TRACE("Created a new checkpoint file: %s", file_name.c_str());
}

// Only called when checkpoint has actual contents
void SimpleCheckpoint::Persist() {
  if (disable_file_access) return;
  PL_ASSERT(file_handle_.file);
  PL_ASSERT(file_handle_.fd != INVALID_FILE_DESCRIPTOR);

  LOG_TRACE("Persisting %lu checkpoint entries", records_.size());
  // write all the record in the queue and free them
  for (auto record : records_) {
    PL_ASSERT(record);
    PL_ASSERT(record->GetMessageLength() > 0);
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           file_handle_.file);
    record.reset();
  }
  records_.clear();
}

void SimpleCheckpoint::Cleanup() {
  // Clean up the record queue
  for (auto record : records_) {
    record.reset();
  }
  records_.clear();

  if (!disable_file_access) {
	//Close and sync the current one
    fclose(file_handle_.file);

    // Remove previous version
    if (checkpoint_version > 0 && !disable_file_access) {
      auto previous_version =
          ConcatFileName(checkpoint_dir, checkpoint_version - 1).c_str();
      if (remove(previous_version) != 0) {
        LOG_TRACE("Failed to remove file %s", previous_version);
      }
    }
  }
  // Truncate logs
  LogManager::GetInstance().TruncateLogs(start_commit_id_);
}

void SimpleCheckpoint::InitVersionNumber() {
  // Get checkpoint version
  LOG_TRACE("Trying to read checkpoint directory");
  struct dirent *file;
  auto dirp = opendir(checkpoint_dir.c_str());
  if (dirp == nullptr) {
    LOG_TRACE("Opendir failed: Errno: %d, error: %s", errno, strerror(errno));
    return;
  }

  while ((file = readdir(dirp)) != NULL) {
    if (strncmp(file->d_name, FILE_PREFIX.c_str(), FILE_PREFIX.length()) == 0) {
      // found a checkpoint file!
      LOG_TRACE("Found a checkpoint file with name %s", file->d_name);
      int version = LoggingUtil::ExtractNumberFromFileName(file->d_name);
      if (version > checkpoint_version) {
        checkpoint_version = version;
      }
    }
  }
  closedir(dirp);
  LOG_TRACE("set checkpoint version to: %d", checkpoint_version);
}

}  // namespace logging
}  // namespace peloton
