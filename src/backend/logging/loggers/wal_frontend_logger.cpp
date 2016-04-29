
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_frontend_logger.cpp
//
// Identification: src/backend/logging/loggers/wal_frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/pool.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"

#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/checkpoint_tile_scanner.h"
#include "backend/logging/logging_util.h"

#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/executor/executor_context.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

extern CheckpointType peloton_checkpoint_mode;

int logger_id_counter = 0;

//#define LOG_FILE_SWITCH_LIMIT (1024)

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

WriteAheadFrontendLogger::WriteAheadFrontendLogger()
    : WriteAheadFrontendLogger(false) {}

/**
 * @brief Open logfile and file descriptor
 */
WriteAheadFrontendLogger::WriteAheadFrontendLogger(bool for_testing)
    : test_mode_(for_testing) {
  logging_type = LOGGING_TYPE_NVM_WAL;

  // allocate pool
  recovery_pool = new VarlenPool(BACKEND_TYPE_MM);
  if (test_mode_) {
    cur_file_handle.file = nullptr;
  } else {
    LOG_INFO("Log dir before getting ID is %s", peloton_log_directory.c_str());
    SetLoggerID(__sync_fetch_and_add(&logger_id_counter, 1));
    LOG_INFO("Log dir after setting ID is %s", peloton_log_directory.c_str());
    InitLogDirectory();
    InitLogFilesList();
    UpdateMaxDelimiterForRecovery();
    LOG_INFO("Updated Max Delimiter for Recovery as %d",
             (int)max_delimiter_for_recovery);
    cur_file_handle.fd = -1;  // this is a restart or a new start
    max_log_id_file = 0;      // 0 is unused
  }
}

/**
 * @brief close logfile
 */
WriteAheadFrontendLogger::~WriteAheadFrontendLogger() {
  // close the log file
  if (cur_file_handle.file != nullptr) {
    int ret = fclose(cur_file_handle.file);
    if (ret != 0) {
      LOG_ERROR("Error occured while closing LogFile");
    }
  }
  // clean up pool
  delete recovery_pool;
}

/**
 * @brief flush all the log records to the file
 */
void WriteAheadFrontendLogger::FlushLogRecords(void) {
  size_t global_queue_size = global_queue.size();

  bool will_write_to_file;

  // check if we will end up writing something to disk
  will_write_to_file =
      ((max_collected_commit_id != max_flushed_commit_id) || global_queue_size);

  if (will_write_to_file) {
    if (cur_file_handle.fd == -1) {
      this->CreateNewLogFile(false);
    } else if (should_create_new_file) {
      this->CreateNewLogFile(true);
      should_create_new_file = false;
    }
  }

  // First, write all the record in the queue
  for (oid_t global_queue_itr = 0; global_queue_itr < global_queue_size;
       global_queue_itr++) {
    auto &log_buffer = global_queue[global_queue_itr];

    if (!test_mode_) {
      fwrite(log_buffer->GetData(), sizeof(char), log_buffer->GetSize(),
             cur_file_handle.file);
    }

    LOG_INFO("Log buffer get max log id returned %d",
             (int)log_buffer->GetMaxLogId());

    if (log_buffer->GetMaxLogId() > this->max_log_id_file) {
      this->max_log_id_file = log_buffer->GetMaxLogId();
      LOG_INFO("Max log id file so far is %d", (int)this->max_log_id_file);
    }

    // return empty buffer
    auto backend_logger = log_buffer->GetBackendLogger();
    log_buffer->ResetData();
    backend_logger->GrantEmptyBuffer(std::move(log_buffer));
  }

  bool flushed = false;
  if (max_collected_commit_id != max_flushed_commit_id) {
    TransactionRecord delimiter_rec(LOGRECORD_TYPE_ITERATION_DELIMITER,
                                    this->max_collected_commit_id);
    delimiter_rec.Serialize(output_buffer);

    if (!test_mode_) {
      assert(cur_file_handle.fd != -1);
      if (cur_file_handle.fd != -1) {
        fwrite(delimiter_rec.GetMessage(), sizeof(char),
               delimiter_rec.GetMessageLength(), cur_file_handle.file);

        LOG_INFO("Wrote delimiter to log file with commit_id %ld",
                 this->max_collected_commit_id);

        // by moving the fflush and sync here, we ensure that this file will
        // have at least 1 delimiter
        if (Clock::now() > last_flush + flush_frequency) {
          LoggingUtil::FFlushFsync(cur_file_handle);

          last_flush = Clock::now();
          if (this->max_collected_commit_id > max_flushed_commit_id) {
            max_flushed_commit_id = this->max_collected_commit_id;
          }

          fsync_count++;
          flushed = true;
        }

        if (this->max_collected_commit_id > max_delimiter_file) {
          max_delimiter_file = this->max_collected_commit_id;
          LOG_INFO("Max_delimiter_file is now %d", (int)max_delimiter_file);
        }

        if (FileSwitchCondIsTrue()) should_create_new_file = true;
      }
    } else {
      if (Clock::now() > last_flush + flush_frequency) {
        last_flush = Clock::now();
        if (this->max_collected_commit_id > max_flushed_commit_id) {
          max_flushed_commit_id = this->max_collected_commit_id;
        }

        flushed = true;
      }
    }
  }

  /* For now, fflush after every iteration of collecting buffers */
  // Clean up the frontend logger's queue
  global_queue.clear();

  if (flushed) {
    // signal that we have flushed
    LogManager::GetInstance().FrontendLoggerFlushed();
  }
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteAheadFrontendLogger::DoRecovery() {
  // FIXME GetNextCommitId() increments next_cid!!!
  cid_t start_commit_id =
      concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId();
  auto &log_manager = logging::LogManager::GetInstance();
  int num_inserts = 0;
  cid_t global_max_flushed_id_for_recovery;
  log_file_cursor_ = 0;

  global_max_flushed_id_for_recovery =
      log_manager.GetGlobalMaxFlushedIdForRecovery();
  LOG_INFO("Got start_commit_id as %d, global max flushed as %d",
           (int)start_commit_id, (int)global_max_flushed_id_for_recovery);

  // open first file
  OpenNextLogFile();

  // Go over the log file if needed
  bool reached_end_of_log = false;

  // Go over each log record in the log file
  while (reached_end_of_log == false) {
    // Read the first byte to identify log record type
    // If that is not possible, then wrap up recovery
    auto record_type = GetNextLogRecordTypeForRecovery();
    LOG_INFO("Record_type is %d", (int)record_type);
    cid_t commit_id = INVALID_CID;
    TupleRecord *tuple_record;

    switch (record_type) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      case LOGRECORD_TYPE_ITERATION_DELIMITER: {
        // Check for torn log write
        TransactionRecord txn_rec(record_type);
        if (LoggingUtil::ReadTransactionRecordHeader(
                txn_rec, cur_file_handle) == false) {
          cur_file_handle = INVALID_FILE_HANDLE;
          return;
        }
        commit_id = txn_rec.GetTransactionId();
        if (commit_id <= start_commit_id ||
            commit_id > global_max_flushed_id_for_recovery) {
          LOG_INFO("SKIP");
          continue;
        }
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
        num_inserts++;
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        if (LoggingUtil::ReadTupleRecordHeader(*tuple_record,
                                               cur_file_handle) == false) {
          LOG_ERROR("Could not read tuple record header.");
          cur_file_handle = INVALID_FILE_HANDLE;
          return;
        }

        auto cid = tuple_record->GetTransactionId();
        auto table = LoggingUtil::GetTable(*tuple_record);

        if (!table || cid <= start_commit_id ||
            commit_id > global_max_flushed_id_for_recovery) {
          LoggingUtil::SkipTupleRecordBody(cur_file_handle);
          delete tuple_record;
          LOG_INFO("SKIP");
          continue;
        }

        if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
          LOG_ERROR("Insert txd id %d not found in recovery txn table",
                    (int)cid);
          cur_file_handle = INVALID_FILE_HANDLE;
          return;
        }

        // Read off the tuple record body from the log
        tuple_record->SetTuple(LoggingUtil::ReadTupleRecordBody(
            table->GetSchema(), recovery_pool, cur_file_handle));
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        if (LoggingUtil::ReadTupleRecordHeader(*tuple_record,
                                               cur_file_handle) == false) {
          cur_file_handle = INVALID_FILE_HANDLE;
          return;
        }

        auto cid = tuple_record->GetTransactionId();
        if (cid <= start_commit_id ||
            commit_id > global_max_flushed_id_for_recovery) {
          delete tuple_record;
          continue;
        }
        if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
          LOG_TRACE("Delete txd id %d not found in recovery txn table",
                    (int)cid);
          cur_file_handle = INVALID_FILE_HANDLE;
          return;
        }
        break;
      }
      default:
        reached_end_of_log = true;
        break;
    }
    if (!reached_end_of_log) {
      switch (record_type) {
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          assert(commit_id != INVALID_CID);
          StartTransactionRecovery(commit_id);
          break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
          assert(commit_id != INVALID_CID);

          // Now directly commit this transaction. This is safe because we
          // reject commit ids that appear
          // after the persistent commit id before coming here (in the switch
          // case above).
          CommitTransactionRecovery(commit_id);
          break;

        case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
        case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
        case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
          recovery_txn_table[tuple_record->GetTransactionId()].push_back(
              tuple_record);
          break;
        case LOGRECORD_TYPE_ITERATION_DELIMITER: {
          // Do nothing if we hit the delimiter, because the delimiters help
          // us only to find
          // the max persistent commit id, and should be ignored during actual
          // recovery
          break;
        }

        default:
          LOG_INFO("Got Type as TXN_INVALID");
          reached_end_of_log = true;
          break;
      }
    }
  }

  // Finally, abort ACTIVE transactions in recovery_txn_table
  AbortActiveTransactions();

  // After finishing recovery, set the next oid with maximum oid
  // observed during the recovery
  log_manager.UpdateCatalogAndTxnManagers(max_oid, max_cid);

  LOG_INFO("This thread did %d inserts", (int)num_inserts);
  cur_file_handle = INVALID_FILE_HANDLE;
}

void WriteAheadFrontendLogger::RecoverIndex() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  LOG_INFO("Recovering the indexes");
  cid_t cid = txn_manager.GetNextCommitId();

  auto &catalog_manager = catalog::Manager::GetInstance();
  auto database_count = catalog_manager.GetDatabaseCount();

  // loop all databases
  for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
    auto database = catalog_manager.GetDatabase(database_idx);
    auto table_count = database->GetTableCount();

    // loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *target_table = database->GetTable(table_idx);
      assert(target_table);
      LOG_INFO("SeqScan: database oid %u table oid %u: %s", database_idx,
               table_idx, target_table->GetName().c_str());

      if (!RecoverTableIndexHelper(target_table, cid)) {
        break;
      }
    }
  }
}

bool WriteAheadFrontendLogger::RecoverTableIndexHelper(
    storage::DataTable *target_table, cid_t start_cid) {
  auto schema = target_table->GetSchema();
  assert(schema);
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  oid_t current_tile_group_offset = START_OID;
  auto table_tile_group_count = target_table->GetTileGroupCount();
  CheckpointTileScanner scanner;

  while (current_tile_group_offset < table_tile_group_count) {
    // Retrieve a tile group
    auto tile_group = target_table->GetTileGroup(current_tile_group_offset);

    // Retrieve a logical tile
    std::unique_ptr<executor::LogicalTile> logical_tile(
        scanner.Scan(tile_group, column_ids, start_cid));

    // Empty result
    if (!logical_tile) {
      current_tile_group_offset++;
      continue;
    }

    auto tile_group_id = logical_tile->GetColumnInfo(0)
                             .base_tile->GetTileGroup()
                             ->GetTileGroupId();
    LOG_TRACE("Retrieved tile group %lu", tile_group_id);

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          logical_tile.get(), tuple_id);

      // Index update
      {
        // construct a physical tuple from the logical tuple
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        for (auto column_id : column_ids) {
          tuple->SetValue(column_id, cur_tuple.GetValue(column_id),
                          recovery_pool);
        }

        ItemPointer location(tile_group_id, tuple_id);
        InsertIndexEntry(tuple.get(), target_table, location);
      }
    }
    current_tile_group_offset++;
  }
  return true;
}

void WriteAheadFrontendLogger::InsertIndexEntry(storage::Tuple *tuple,
                                                storage::DataTable *table,
                                                ItemPointer target_location) {
  assert(tuple);
  assert(table);
  auto index_count = table->GetIndexCount();
  LOG_TRACE("Insert tuple (%lu, %lu) into %lu indexes", target_location.block,
            target_location.offset, index_count);

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = table->GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    index->InsertEntry(key.get(), target_location);
    // Increase the indexes' number of tuples by 1 as well
    index->IncreaseNumberOfTuplesBy(1);
  }
}

/**
 * @brief Add new txn to recovery table
 */
void WriteAheadFrontendLogger::AbortActiveTransactions() {
  for (auto it = recovery_txn_table.begin(); it != recovery_txn_table.end();
       it++) {
    LOG_INFO("Aborting some active transactions!");
    for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
      delete *it2;
    }
  }
  recovery_txn_table.clear();
}

/**
 * @brief Add new txn to recovery table
 */
void WriteAheadFrontendLogger::StartTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord *> tuple_recs;
  recovery_txn_table[commit_id] = tuple_recs;
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit
 * them later
 * @param recovery txn
 */
void WriteAheadFrontendLogger::CommitTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord *> &tuple_records = recovery_txn_table[commit_id];
  for (auto it = tuple_records.begin(); it != tuple_records.end(); it++) {
    TupleRecord *curr = *it;
    switch (curr->GetType()) {
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
        InsertTuple(curr);
        break;
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
        UpdateTuple(curr);
        break;
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
        DeleteTuple(curr);
        break;
      default:
        continue;
    }
    delete curr;
  }
  max_cid = commit_id + 1;
  recovery_txn_table.erase(commit_id);
}

void InsertTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &insert_loc,
                       storage::Tuple *tuple,
                       bool should_increase_tuple_count = true) {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    delete tuple;
    return;
  }
  assert(table);

  // FIXME Handle the case when tile_group is not created yet.
  // table->GetTileGroupLock().WriteLock();
  auto tile_group = manager.GetTileGroup(insert_loc.block);

  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(insert_loc.block);
    tile_group = manager.GetTileGroup(insert_loc.block);
    if (max_tg < insert_loc.block) {
      max_tg = insert_loc.block;
    }
  }
  // table->GetTileGroupLock().Unlock();
  // unlock table here

  tile_group->InsertTupleFromRecovery(commit_id, insert_loc.offset, tuple);
  if (should_increase_tuple_count) {
    table->GetTileGroupLock().WriteLock();
    table->IncreaseNumberOfTuplesBy(1);
    table->GetTileGroupLock().Unlock();
  }
  delete tuple;
}

void DeleteTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &delete_loc) {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    return;
  }
  assert(table);

  // FIXME Handle the case when tile_group is not created yet.
  // acquiring the write lock will lead to deadlock
  // table->GetTileGroupLock().WriteLock();
  auto tile_group = manager.GetTileGroup(delete_loc.block);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(delete_loc.block);
    tile_group = manager.GetTileGroup(delete_loc.block);
    if (max_tg < delete_loc.block) {
      max_tg = delete_loc.block;
    }
  }
  // FIXME we always decrease the number of tuples by one
  table->DecreaseNumberOfTuplesBy(1);
  // table->GetTileGroupLock().Unlock();

  tile_group->DeleteTupleFromRecovery(commit_id, delete_loc.offset);
}

void UpdateTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &remove_loc,
                       const ItemPointer &insert_loc, storage::Tuple *tuple) {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    delete tuple;
    return;
  }
  assert(table);

  // FIXME Handle the case when tile_group is not created yet.
  // table->GetTileGroupLock().WriteLock();
  auto tile_group = manager.GetTileGroup(remove_loc.block);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(remove_loc.block);
    tile_group = manager.GetTileGroup(remove_loc.block);
    if (max_tg < remove_loc.block) {
      max_tg = remove_loc.block;
    }
  }
  // table->GetTileGroupLock().Unlock();
  InsertTupleHelper(max_tg, commit_id, db_id, table_id, insert_loc, tuple,
                    false);

  tile_group->UpdateTupleFromRecovery(commit_id, remove_loc.offset, insert_loc);
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void WriteAheadFrontendLogger::InsertTuple(TupleRecord *record) {
  InsertTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetInsertLocation(), record->GetTuple());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void WriteAheadFrontendLogger::DeleteTuple(TupleRecord *record) {
  DeleteTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetDeleteLocation());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */

void WriteAheadFrontendLogger::UpdateTuple(TupleRecord *record) {
  UpdateTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetDeleteLocation(), record->GetInsertLocation(),
                    record->GetTuple());
}

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

LogRecordType WriteAheadFrontendLogger::GetNextLogRecordTypeForRecovery() {
  char buffer;
  bool is_truncated = false;
  int ret;

  if (cur_file_handle.file == nullptr || cur_file_handle.fd == -1)
    return LOGRECORD_TYPE_INVALID;

  LOG_INFO("Inside GetNextLogRecordForRecovery");

  LOG_INFO("File is at position %d", (int)ftell(cur_file_handle.file));

  // Check if the log record type is broken
  if (LoggingUtil::IsFileTruncated(cur_file_handle, 1)) {
    LOG_INFO("Log file is truncated, should open next log file");
    is_truncated = true;
  }

  // Otherwise, read the log record type
  if (!is_truncated) {
    ret = fread((void *)&buffer, 1, sizeof(char), cur_file_handle.file);
    if (ret <= 0) {
      LOG_INFO("Failed an fread");
    }
  }
  if (is_truncated || ret <= 0) {
    LOG_INFO("Call OpenNextLogFile");
    OpenNextLogFile();
    if (cur_file_handle.fd == -1) return LOGRECORD_TYPE_INVALID;

    LOG_INFO("Open succeeded. log_file_fd is %d", (int)cur_file_handle.fd);

    if (LoggingUtil::IsFileTruncated(cur_file_handle, 1)) {
      LOG_ERROR("Log file is truncated");
      return LOGRECORD_TYPE_INVALID;
    }

    LOG_INFO("File is not truncated.");
    ret = fread((void *)&buffer, 1, sizeof(char), cur_file_handle.file);
    if (ret <= 0) {
      LOG_ERROR("Could not read from log file");
      return LOGRECORD_TYPE_INVALID;
    }
    LOG_INFO("fread succeeded.");
  } else {
    LOG_INFO("fread succeeded.");
  }

  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

std::string WriteAheadFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

bool CompareByLogNumber(class LogFile *left, class LogFile *right) {
  return left->GetLogNumber() < right->GetLogNumber();
}

void WriteAheadFrontendLogger::InitLogFilesList() {
  struct dirent *file;
  DIR *dirp;
  cid_t temp_max_log_id_file, temp_max_delimiter_file;
  int version_number, ret_val;
  FILE *fp;
  std::pair<cid_t, cid_t> extracted_values;
  FileHandle temp_file_handle;

  // TODO need a better regular expression to match file name
  std::string base_name = "peloton_log_";

  LOG_INFO("Trying to read log directory");

  dirp = opendir(this->peloton_log_directory.c_str());
  if (dirp == nullptr) {
    LOG_INFO("Opendir failed: Errno: %d, error: %s", errno, strerror(errno));
  }

  // XXX readdir is not thread safe???
  while ((file = readdir(dirp)) != NULL) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // found a log file!
      LOG_INFO("Found a log file with name %s", file->d_name);

      version_number = LoggingUtil::ExtractNumberFromFileName(file->d_name);

      fp = fopen(GetFileNameFromVersion(version_number).c_str(), "rb+");
      temp_max_log_id_file = UINT64_MAX;
      temp_max_delimiter_file = 0;

      size_t read_size = fread((void *)&temp_max_log_id_file,
                               sizeof(temp_max_log_id_file), 1, fp);
      if (read_size != 1) {
        LOG_ERROR("Read from file %s failed",
                  GetFileNameFromVersion(version_number).c_str());
        fclose(fp);
        continue;
      }
      LOG_INFO("Got temp_max_log_id_file as %d", (int)temp_max_log_id_file);

      read_size = fread((void *)&temp_max_delimiter_file,
                        sizeof(temp_max_delimiter_file), 1, fp);
      if (read_size != 1) {
        LOG_ERROR("Read from file %s failed",
                  GetFileNameFromVersion(version_number).c_str());
        fclose(fp);
        continue;
      }
      LOG_INFO("Got temp_max_delimiter_file as %d",
               (int)temp_max_delimiter_file);

      if (temp_max_log_id_file == 0 || temp_max_log_id_file == UINT64_MAX ||
          temp_max_delimiter_file == 0) {
        extracted_values = ExtractMaxLogIdAndMaxDelimFromLogFileRecords(fp);

        temp_max_log_id_file = extracted_values.first;
        temp_max_delimiter_file = extracted_values.second;

        LOG_INFO("ExtractMaxLogId returned %d, write it back in the file!",
                 (int)temp_max_log_id_file);
        LOG_INFO("ExtractMaxDelim returned %d, write it back in the file!",
                 (int)temp_max_delimiter_file);

        ret_val = fseek(fp, 0, SEEK_SET);
        if (ret_val != 0) {
          LOG_ERROR("Could not seek to the beginning of file: %s",
                    strerror(errno));
          fclose(fp);
          continue;
        }
        ret_val = fwrite((void *)&(temp_max_log_id_file),
                         sizeof(temp_max_log_id_file), 1, fp);

        if (ret_val <= 0) {
          LOG_ERROR("Could not write Max Log ID to file header: %s",
                    strerror(errno));
          fclose(fp);
          continue;
        }

        ret_val = fwrite((void *)&(temp_max_delimiter_file),
                         sizeof(temp_max_delimiter_file), 1, fp);

        if (ret_val <= 0) {
          LOG_ERROR("Could not write Max Delimiter to file header: %s",
                    strerror(errno));
          fclose(fp);
          continue;
        }
      }

      fclose(fp);

      // TODO update max_delimiter here in this constructor
      temp_file_handle.fd = -1;
      temp_file_handle.file = NULL;
      temp_file_handle.size = LoggingUtil::GetFileSizeFromFileName(
          GetFileNameFromVersion(version_number).c_str());

      LogFile *new_log_file =
          new LogFile(temp_file_handle, file->d_name, version_number,
                      temp_max_log_id_file, temp_max_delimiter_file);

      this->log_files_.push_back(new_log_file);
    }
  }
  closedir(dirp);

  int num_log_files;
  num_log_files = this->log_files_.size();

  LOG_INFO("The number of log files found: %d", num_log_files);

  std::sort(this->log_files_.begin(), this->log_files_.end(),
            CompareByLogNumber);

  if (num_log_files) {
    int max_num = LoggingUtil::ExtractNumberFromFileName(
        this->log_files_[num_log_files - 1]->GetLogFileName().c_str());
    LOG_INFO("Got maximum log file version as %d", max_num);
    this->log_file_counter_ = ++max_num;
  } else {
    this->log_file_counter_ = 0;
  }
}

void WriteAheadFrontendLogger::CreateNewLogFile(bool close_old_file) {
  if (test_mode_) {
    return;
  }

  int new_file_num;
  std::string new_file_name;
  cid_t default_commit_id = INVALID_CID, default_delimiter = INVALID_CID;
  struct stat log_stats;

  new_file_num = log_file_counter_;

  if (close_old_file) {  // must close last opened file
    int file_list_size = log_files_.size();
    LogFile *cur_log_file_object = log_files_[file_list_size - 1];

    if (file_list_size != 0) {
      // TODO check return values of all these operations!
      fseek(cur_file_handle.file, 0, SEEK_SET);

      fwrite((void *)&(max_log_id_file), sizeof(max_log_id_file), 1,
             cur_file_handle.file);

      cur_log_file_object->SetMaxLogId(max_log_id_file);

      LOG_INFO("MaxLogID of the last closed file is %d", (int)max_log_id_file);

      fwrite((void *)&(max_delimiter_file), sizeof(max_delimiter_file), 1,
             cur_file_handle.file);

      cur_log_file_object->SetMaxDelimiter(max_delimiter_file);

      LOG_INFO("MaxDelimiter of the last closed file is %d",
               (int)max_delimiter_file);

      max_log_id_file = 0;     // reset
      max_delimiter_file = 0;  // reset

      fstat(cur_file_handle.fd, &log_stats);

      cur_file_handle.size = log_stats.st_size;

      LOG_INFO("The log file to be closed has size %d",
               (int)cur_file_handle.size);

      cur_log_file_object->SetLogFileSize(cur_file_handle.size);

      fclose(cur_file_handle.file);

      cur_log_file_object->SetFilePtr(nullptr);  // invalidate
      cur_log_file_object->SetLogFileFD(-1);     // invalidate
    }
  }

  LOG_INFO("new_file_num is %d", new_file_num);

  new_file_name = this->GetFileNameFromVersion(new_file_num);

  FILE *new_log_file = fopen(new_file_name.c_str(), "wb");
  if (new_log_file == NULL) {
    LOG_ERROR("new_log_file is NULL");
    return;
  }

  // now set the first 8 bytes to 0 - this is for the max_log id in this file
  fwrite((void *)&default_commit_id, sizeof(default_commit_id), 1,
         new_log_file);

  // now set the next 8 bytes to 0 - this is for the max delimiter in this file
  fwrite((void *)&default_delimiter, sizeof(default_delimiter), 1,
         new_log_file);

  cur_file_handle.file = new_log_file;
  cur_file_handle.fd = fileno(cur_file_handle.file);
  cur_file_handle.size = 0;

  if (cur_file_handle.fd == -1) {
    LOG_ERROR("cur_file_handle.fd is -1");
  }

  LOG_INFO("FD of newly created file is %d", cur_file_handle.fd);

  LogFile *new_log_file_object = new LogFile(
      cur_file_handle, new_file_name, new_file_num, INVALID_CID, INVALID_CID);

  log_files_.push_back(new_log_file_object);

  log_file_counter_++;  // finally, increment log_file_counter_

  LOG_INFO("log_file_counter is %d", log_file_counter_);
}

bool WriteAheadFrontendLogger::FileSwitchCondIsTrue() {
  struct stat stat_buf;
  if (cur_file_handle.fd == -1) return false;

  fstat(cur_file_handle.fd, &stat_buf);
  cur_file_handle.size = stat_buf.st_size;

  return cur_file_handle.size >
         LogManager::GetInstance().GetLogFileSizeLimit() * 1024;
}

void WriteAheadFrontendLogger::OpenNextLogFile() {
  cid_t temp_max_log_id_file, temp_max_delimiter_file;

  if (log_files_.size() == 0) {  // no log files, fresh start
    LOG_INFO("Size of log files list is 0.");
    cur_file_handle = INVALID_FILE_HANDLE;
    return;
  }

  if (this->log_file_cursor_ >= (int)this->log_files_.size()) {
    LOG_INFO("Cursor has reached the end. No more log files to read from.");
    cur_file_handle = INVALID_FILE_HANDLE;
    return;
  }

  if (log_file_cursor_ != 0)  // close old file
  {
    LOG_INFO("Closing last opened file");
    fclose(cur_file_handle.file);
  }

  // open the next file
  cur_file_handle.file =
      fopen(GetFileNameFromVersion(log_files_[log_file_cursor_]->GetLogNumber())
                .c_str(),
            "rb");

  if (cur_file_handle.file == NULL) {
    LOG_ERROR("Couldn't open next log file");
    cur_file_handle = INVALID_FILE_HANDLE;
    return;
  } else {
    LOG_INFO("Opened new log file for recovery");
  }

  cur_file_handle.fd = fileno(cur_file_handle.file);

  LOG_INFO("FD of opened file is %d", (int)cur_file_handle.fd);

  // Skip first 8 bytes of max commit id
  size_t read_size =
      fread((void *)&temp_max_log_id_file, sizeof(temp_max_log_id_file), 1,
            cur_file_handle.file);
  if (read_size != 1) {
    LOG_ERROR("Read failed after opening file %s",
              GetFileNameFromVersion(
                  log_files_[log_file_cursor_]->GetLogNumber()).c_str());
  }

  LOG_INFO("On startup: MaxLogId of this file is %d",
           (int)temp_max_log_id_file);

  // Skip next 8 bytes of max delimiter
  read_size = fread((void *)&temp_max_delimiter_file,
                    sizeof(temp_max_delimiter_file), 1, cur_file_handle.file);

  if (read_size != 1) {
    LOG_ERROR("Read failed after opening file %s",
              GetFileNameFromVersion(
                  log_files_[log_file_cursor_]->GetLogNumber()).c_str());
  }

  LOG_INFO("On startup: MaxDelimiter of this file is %d",
           (int)temp_max_delimiter_file);

  struct stat stat_buf;

  fstat(cur_file_handle.fd, &stat_buf);
  cur_file_handle.size = stat_buf.st_size;

  log_file_cursor_++;
  LOG_INFO("Cursor is now %d", (int)log_file_cursor_);
}

void WriteAheadFrontendLogger::TruncateLog(cid_t truncate_log_id) {
  int return_val;

  // delete stale log files except the one currently being used
  for (int i = 0; i < (int)log_files_.size() - 1; i++) {
    if (truncate_log_id >= log_files_[i]->GetMaxLogId()) {
      // XXX Do we need directory prefix before log file name?
      return_val = remove(log_files_[i]->GetLogFileName().c_str());
      if (return_val != 0) {
        LOG_ERROR("Couldn't delete log file: %s error: %s",
                  log_files_[i]->GetLogFileName().c_str(), strerror(errno));
      }
      // remove entry from list anyway
      delete log_files_[i];
      log_files_.erase(log_files_.begin() + i);
      i--;  // update cursor
    }
  }
}

void WriteAheadFrontendLogger::InitLogDirectory() {
  auto success =
      LoggingUtil::CreateDirectory(peloton_log_directory.c_str(), 0700);
  if (success) {
    LOG_INFO("Logging directory is: %s", peloton_log_directory.c_str());
  } else {
    LOG_ERROR("Failed to create logging directory");
  }
}

// TODO should we probably implement this
void WriteAheadFrontendLogger::SetLogDirectory(char *arg
                                               __attribute__((unused))) {
  LOG_INFO("%s", arg);
}

std::string WriteAheadFrontendLogger::GetFileNameFromVersion(int version) {
  return std::string(peloton_log_directory.c_str()) + "/" + LOG_FILE_PREFIX +
         std::to_string(version) + LOG_FILE_SUFFIX;
}

std::pair<cid_t, cid_t>
WriteAheadFrontendLogger::ExtractMaxLogIdAndMaxDelimFromLogFileRecords(
    FILE *log_file) {
  bool reached_end_of_file = false;
  struct stat log_stats;
  cid_t max_log_id_so_far = 0, max_delim_so_far = 0;
  FileHandle file_handle;

  file_handle.file = log_file;
  file_handle.fd = fileno(log_file);

  fstat(file_handle.fd, &log_stats);
  file_handle.size = log_stats.st_size;

  while (reached_end_of_file == false) {
    // Read the first byte to identify log record type
    // If that is not possible, then wrap up recovery
    auto record_type = LoggingUtil::GetNextLogRecordType(file_handle);

    cid_t commit_id = INVALID_CID;

    TupleRecord *tuple_record;

    switch (record_type) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      case LOGRECORD_TYPE_ITERATION_DELIMITER: {
        // Check for torn log write
        TransactionRecord txn_rec(record_type);
        if (LoggingUtil::ReadTransactionRecordHeader(txn_rec, file_handle) ==
            false) {
          return std::pair<cid_t, cid_t>(UINT64_MAX, UINT64_MAX);
        }
        commit_id = txn_rec.GetTransactionId();
        if (commit_id > max_log_id_so_far) max_log_id_so_far = commit_id;

        if (record_type == LOGRECORD_TYPE_ITERATION_DELIMITER) {
          if (commit_id > max_delim_so_far) max_delim_so_far = commit_id;
        }
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
        tuple_record = new TupleRecord(record_type);

        if (LoggingUtil::ReadTupleRecordHeader(*tuple_record, file_handle) ==
            false) {
          LOG_ERROR("Could not read tuple record header.");
          delete tuple_record;
          return std::pair<cid_t, cid_t>(UINT64_MAX, UINT64_MAX);
        }

        auto cid = tuple_record->GetTransactionId();

        if (cid > max_log_id_so_far) max_log_id_so_far = cid;

        auto table = LoggingUtil::GetTable(*tuple_record);
        if (!table) {
          LoggingUtil::SkipTupleRecordBody(file_handle);
          delete tuple_record;
          continue;
        }

        // Read off the tuple record body from the log
        LoggingUtil::ReadTupleRecordBody(table->GetSchema(), recovery_pool,
                                         file_handle);
        delete tuple_record;

        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
        tuple_record = new TupleRecord(record_type);

        if (LoggingUtil::ReadTupleRecordHeader(*tuple_record, file_handle) ==
            false) {
          delete tuple_record;
          return std::pair<cid_t, cid_t>(UINT64_MAX, UINT64_MAX);
        }

        auto cid = tuple_record->GetTransactionId();

        if (cid > max_log_id_so_far) max_log_id_so_far = cid;
        delete tuple_record;
        break;
      }
      default:
        reached_end_of_file = true;
        break;
    }
  }
  return std::pair<cid_t, cid_t>(max_log_id_so_far, max_delim_so_far);
}

void WriteAheadFrontendLogger::SetLoggerID(int id) {
  logger_id = id;
  peloton_log_directory += std::to_string(id);
}

void WriteAheadFrontendLogger::UpdateMaxDelimiterForRecovery() {
  // this method must update the max delimiter id to be used for recovery
  int num_log_files = log_files_.size();
  cid_t max_delimiter_last_file;

  if (num_log_files == 0) {
    return;  // no delimiter, default is 0
  }

  max_delimiter_last_file = log_files_[num_log_files - 1]->GetMaxDelimiter();

  if (max_delimiter_last_file != 0) {
    // found a delimiter in the last file! use it to update max delimiter for
    // recovery
    max_delimiter_for_recovery = max_delimiter_last_file;
    return;
  }

  // we didn't find a delimiter in the last file

  if (num_log_files == 1) {
    return;  // no other file to pick up delimiter from, return
  }

  // take delimiter from the last-but-one file!
  max_delimiter_for_recovery = log_files_[num_log_files - 2]->GetMaxDelimiter();
}

}  // namespace logging
}  // namespace peloton
