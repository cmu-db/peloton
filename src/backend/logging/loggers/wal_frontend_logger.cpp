
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

#define LOG_FILE_SWITCH_LIMIT (1024)

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

size_t GetLogFileSize(int log_file_fd);

bool IsFileTruncated(FILE *log_file, size_t size_to_read, size_t log_file_size);

size_t GetNextFrameSize(FILE *log_file, size_t log_file_size);

bool ReadTransactionRecordHeader(TransactionRecord &txn_record, FILE *log_file,
                                 size_t log_file_size);

bool ReadTupleRecordHeader(TupleRecord &tuple_record, FILE *log_file,
                           size_t log_file_size);

storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema, VarlenPool *pool,
                                    FILE *log_file, size_t log_file_size);

void SkipTupleRecordBody(FILE *log_file, size_t log_file_size);

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

// Wrappers
storage::DataTable *GetTable(TupleRecord &tupleRecord);

int ExtractNumberFromFileName(const char *name);

/**
 * @brief Open logfile and file descriptor
 */

WriteAheadFrontendLogger::WriteAheadFrontendLogger()
    : WriteAheadFrontendLogger(false) {}

/**
 * @brief Open logfile and file descriptor
 */

WriteAheadFrontendLogger::WriteAheadFrontendLogger(bool for_testing) {
  logging_type = LOGGING_TYPE_DRAM_NVM;

  /*LOG_INFO("Log File Name :: %s", GetLogFileName().c_str());

  // open log file and file descriptor
  // we open it in append + binary mode
  log_file = fopen(GetLogFileName().c_str(), "ab+");
  if (log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }*/

  // allocate pool
  recovery_pool = new VarlenPool(BACKEND_TYPE_MM);
  if (for_testing) {
    this->log_file = nullptr;

  } else {
    LOG_INFO("Log dir is %s", this->peloton_log_directory.c_str());
    this->InitLogDirectory();
    this->InitLogFilesList();
    this->log_file_fd = -1;     // this is a restart or a new start
    this->max_log_id_file = 0;  // 0 is unused
  }
}

/**
 * @brief close logfile
 */
WriteAheadFrontendLogger::~WriteAheadFrontendLogger() {
  // close the log file
  if (log_file != nullptr) {
    int ret = fclose(log_file);
    if (ret != 0) {
      LOG_ERROR("Error occured while closing LogFile");
    }
  }
  // clean up pool
  delete recovery_pool;
}

void fflush_and_sync(FILE *log_file, int log_file_fd, size_t &fsync_count) {
  // First, flush
  assert(log_file_fd != -1);
  if (log_file_fd == -1) return;

  int ret = fflush(log_file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(log_file_fd);
  fsync_count++;
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}
/**
 * @brief flush all the log records to the file
 */
void WriteAheadFrontendLogger::FlushLogRecords(void) {
  size_t global_queue_size = global_queue.size();

  // First, write all the record in the queue
  if (global_queue_size != 0 && this->log_file_fd == -1) {
    this->CreateNewLogFile(false);
  }

  // LOG_INFO("Flushing %lu log buffers..", global_queue_size);
  for (oid_t global_queue_itr = 0; global_queue_itr < global_queue_size;
       global_queue_itr++) {
    auto &log_buffer = global_queue[global_queue_itr];

    fwrite(log_buffer->GetData(), sizeof(char), log_buffer->GetSize(),
           log_file);
    // TODO this is not correct and must be fixed, should be max seen cid
    if (log_buffer->GetHighestCommittedTransaction() > this->max_log_id_file) {
      this->max_log_id_file = log_buffer->GetHighestCommittedTransaction();

      LOG_INFO("MaxSoFar is %d", (int)this->max_log_id_file);
    }

    // return empty buffer
    auto backend_logger = log_buffer->GetBackendLogger();
    log_buffer->ResetData();
    backend_logger->GrantEmptyBuffer(std::move(log_buffer));
  }

  if (max_collected_commit_id != max_flushed_commit_id) {
    TransactionRecord delimiter_rec(LOGRECORD_TYPE_ITERATION_DELIMITER,
                                    this->max_collected_commit_id);
    delimiter_rec.Serialize(output_buffer);

    assert(log_file_fd != -1);

    if (log_file_fd != -1) {
      fwrite(delimiter_rec.GetMessage(), sizeof(char),
             delimiter_rec.GetMessageLength(), log_file);

      LOG_INFO("Wrote delimiter to log file with commit_id %ld",
               this->max_collected_commit_id);

      // by moving the fflush and sync here, we ensure that this file will have
      // at least 1 delimiter
      fflush_and_sync(log_file, log_file_fd, fsync_count);

      if (this->max_collected_commit_id > max_delimiter_file)
      {
        max_delimiter_file = this->max_collected_commit_id;
	LOG_INFO("Max_delimiter_file is now %d", (int)max_delimiter_file);
      }

      if (this->FileSwitchCondIsTrue()) this->CreateNewLogFile(true);
    }
  }

  /* For now, fflush after every iteration of collecting buffers */
  // Clean up the frontend logger's queue
  global_queue.clear();

  // Commit each backend logger
  if (this->max_collected_commit_id > max_flushed_commit_id) {
    max_flushed_commit_id = this->max_collected_commit_id;
  }
  // signal that we have flushed
  LogManager::GetInstance().FrontendLoggerFlushed();
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteAheadFrontendLogger::DoRecovery() {

  //FIXME GetNextCommitId() increments next_cid!!!
  cid_t start_commit_id = concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId();

  log_file_cursor_ = 0;

  // Set log file size
  // log_file_size = GetLogFileSize(log_file_fd);
  this->OpenNextLogFile();

  // Go over the log size if needed
  if (log_file_size > 0) {
    bool reached_end_of_file = false;
    __attribute__((unused)) oid_t recovery_log_record_count = 0;

    // Go over each log record in the log file
    while (reached_end_of_file == false) {
      // Read the first byte to identify log record type
      // If that is not possible, then wrap up recovery
      auto record_type = this->GetNextLogRecordTypeForRecovery();
      LOG_INFO("Record_type is %d", (int)record_type);
      cid_t commit_id = INVALID_CID;
      TupleRecord *tuple_record;
      switch (record_type) {
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        case LOGRECORD_TYPE_ITERATION_DELIMITER: {
          // Check for torn log write
          TransactionRecord txn_rec(record_type);
          if (ReadTransactionRecordHeader(txn_rec, log_file, log_file_size) ==
              false) {
            this->log_file_fd = -1;
            return;
          }
          commit_id = txn_rec.GetTransactionId();
          if (commit_id <= start_commit_id) {
            continue;
          }
          break;
        }
        case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
        case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
          tuple_record = new TupleRecord(record_type);
          // Check for torn log write
          if (ReadTupleRecordHeader(*tuple_record, log_file, log_file_size) ==
              false) {
            LOG_ERROR("Could not read tuple record header.");
            this->log_file_fd = -1;
            return;
          }

          auto cid = tuple_record->GetTransactionId();
          auto table = GetTable(*tuple_record);
          if (!table || cid <= start_commit_id) {
            SkipTupleRecordBody(log_file, log_file_size);
            delete tuple_record;
            continue;
          }

          if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
            LOG_ERROR("Insert txd id %d not found in recovery txn table",
                      (int)cid);
            this->log_file_fd = -1;
            return;
          }

          // Read off the tuple record body from the log
          tuple_record->SetTuple(ReadTupleRecordBody(
              table->GetSchema(), recovery_pool, log_file, log_file_size));
          break;
        }
        case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
          tuple_record = new TupleRecord(record_type);
          // Check for torn log write
          if (ReadTupleRecordHeader(*tuple_record, log_file, log_file_size) ==
              false) {
            this->log_file_fd = -1;
            return;
          }

          auto cid = tuple_record->GetTransactionId();
          if (cid <= start_commit_id) {
            delete tuple_record;
            continue;
          }
          if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
            LOG_TRACE("Delete txd id %d not found in recovery txn table",
                      (int)cid);
            this->log_file_fd = -1;
            return;
          }
          break;
        }
        default:
          reached_end_of_file = true;
          break;
      }
      if (!reached_end_of_file) {
        switch (record_type) {
          case LOGRECORD_TYPE_TRANSACTION_BEGIN:
            assert(commit_id != INVALID_CID);
            StartTransactionRecovery(commit_id);
            break;

          case LOGRECORD_TYPE_TRANSACTION_COMMIT:
            assert(commit_id != INVALID_CID);
            pending_commits.insert(commit_id);
            break;

          case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
          case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
          case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
            recovery_txn_table[tuple_record->GetTransactionId()].push_back(
                tuple_record);
            break;
          case LOGRECORD_TYPE_ITERATION_DELIMITER: {
            auto it = pending_commits.begin();
            for (; it != pending_commits.end(); it++) {
              cid_t curr = *it;
              if (curr > commit_id) {
                break;
              }
              CommitTransactionRecovery(curr);
            }
            if (it != pending_commits.begin()) {
              pending_commits.erase(pending_commits.begin(), it);
            }
            break;
          }

          default:
            LOG_INFO("Got Type as TXN_INVALID");
            reached_end_of_file = true;
            break;
        }
      }
    }

    // Finally, abort ACTIVE transactions in recovery_txn_table
    AbortActiveTransactions();
    pending_commits.clear();

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    if (max_oid > manager.GetNextOid()) {
      manager.SetNextOid(max_oid);
    }

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    if (txn_manager.GetNextCommitId() < max_cid) {
      txn_manager.SetNextCid(max_cid + 1);
    }

    RecoverIndex();
  }
  this->log_file_fd = -1;
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
      LOG_INFO("SeqScan: database oid %lu table oid %lu: %s", database_idx,
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
  auto tile_group = manager.GetTileGroup(insert_loc.block);
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    delete tuple;
    return;
  }
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(insert_loc.block);
    tile_group = manager.GetTileGroup(insert_loc.block);
    if (max_tg < insert_loc.block) {
      max_tg = insert_loc.block;
    }
  }

  tile_group->InsertTupleFromRecovery(commit_id, insert_loc.offset, tuple);
  if (should_increase_tuple_count) {
    table->IncreaseNumberOfTuplesBy(1);
  }
  delete tuple;
}

void DeleteTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &delete_loc) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(delete_loc.block);
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    return;
  }
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(delete_loc.block);
    tile_group = manager.GetTileGroup(delete_loc.block);
    if (max_tg < delete_loc.block) {
      max_tg = delete_loc.block;
    }
  }
  table->DecreaseNumberOfTuplesBy(1);
  tile_group->DeleteTupleFromRecovery(commit_id, delete_loc.offset);
}

void UpdateTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &remove_loc,
                       const ItemPointer &insert_loc, storage::Tuple *tuple) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(remove_loc.block);
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  if (!table) {
    delete tuple;
    return;
  }
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(remove_loc.block);
    tile_group = manager.GetTileGroup(remove_loc.block);
    if (max_tg < remove_loc.block) {
      max_tg = remove_loc.block;
    }
  }
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

/**
 * @brief Measure the size of log file
 * @return the size if the log file exists otherwise 0
 */
size_t GetLogFileSize(int log_file_fd) {
  struct stat log_stats;

  fstat(log_file_fd, &log_stats);
  return log_stats.st_size;
}

bool IsFileTruncated(FILE *log_file, size_t size_to_read,
                     size_t log_file_size) {
  // Cache current position
  size_t current_position = ftell(log_file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if (current_position + size_to_read <= log_file_size) {
    return false;
  } else {
    fseek(log_file, 0, SEEK_END);
    return true;
  }
}

/**
 * @brief get the next frame size
 *  TupleRecord consiss of two frame ( header and Body)
 *  Transaction Record has a single frame
 * @return the next frame size
 */
size_t GetNextFrameSize(FILE *log_file, size_t log_file_size) {
  size_t frame_size;
  char buffer[sizeof(int32_t)];

  // Check if the frame size is broken
  if (IsFileTruncated(log_file, sizeof(buffer), log_file_size)) {
    return 0;
  }

  // Otherwise, read the frame size
  size_t ret = fread(buffer, 1, sizeof(buffer), log_file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInputBE frameCheck(buffer, sizeof(buffer));
  frame_size = (frameCheck.ReadInt()) + sizeof(buffer);

  // Move back by 4 bytes
  // So that tuple deserializer works later as expected
  int res = fseek(log_file, -sizeof(buffer), SEEK_CUR);
  if (res == -1) {
    LOG_ERROR("Error occured in fseek ");
  }

  // Check if the frame is broken
  if (IsFileTruncated(log_file, frame_size, log_file_size)) {
    return 0;
  }

  return frame_size;
}

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return log record type otherwise return invalid log record type,
 * which menas there is no more log in the log file
 */

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size) {
  char buffer;

  // Check if the log record type is broken
  if (IsFileTruncated(log_file, 1, log_file_size)) {
    LOG_INFO("Log file is truncated");
    return LOGRECORD_TYPE_INVALID;
  }

  // Otherwise, read the log record type
  int ret = fread((void *)&buffer, 1, sizeof(char), log_file);
  if (ret <= 0) {
    LOG_ERROR("Could not read from log file");
    return LOGRECORD_TYPE_INVALID;
  }

  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

LogRecordType WriteAheadFrontendLogger::GetNextLogRecordTypeForRecovery() {
  char buffer;
  bool is_truncated = false;
  int ret;

  LOG_INFO("Inside GetNextLogRecordForRecovery");

  LOG_INFO("File is at position %d", (int)ftell(this->log_file));
  // Check if the log record type is broken
  if (IsFileTruncated(this->log_file, 1, this->log_file_size)) {
    LOG_INFO("Log file is truncated, should open next log file");
    // return LOGRECORD_TYPE_INVALID;
    is_truncated = true;
  }

  // Otherwise, read the log record type
  if (!is_truncated) {
    ret = fread((void *)&buffer, 1, sizeof(char), this->log_file);
    if (ret <= 0) {
      LOG_INFO("Failed an fread");
    }
  }
  if (is_truncated || ret <= 0) {
    LOG_INFO("Call OpenNextLogFile");
    this->OpenNextLogFile();
    if (this->log_file_fd == -1) return LOGRECORD_TYPE_INVALID;

    LOG_INFO("Open succeeded. log_file_fd is %d", (int)this->log_file_fd);

    if (IsFileTruncated(this->log_file, 1, this->log_file_size)) {
      LOG_ERROR("Log file is truncated");
      return LOGRECORD_TYPE_INVALID;
    }
    LOG_INFO("File is not truncated.");
    ret = fread((void *)&buffer, 1, sizeof(char), this->log_file);
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

/**
 * @brief Read TransactionRecord
 * @param txn_record
 */
bool ReadTransactionRecordHeader(TransactionRecord &txn_record, FILE *log_file,
                                 size_t log_file_size) {
  // Check if frame is broken
  auto header_size = GetNextFrameSize(log_file, log_file_size);
  if (header_size == 0) {
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, sizeof(header), log_file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInputBE txn_header(header, header_size);
  txn_record.Deserialize(txn_header);

  return true;
}

/**
 * @brief Read TupleRecordHeader
 * @param tuple_record
 */
bool ReadTupleRecordHeader(TupleRecord &tuple_record, FILE *log_file,
                           size_t log_file_size) {
  // Check if frame is broken
  auto header_size = GetNextFrameSize(log_file, log_file_size);
  if (header_size == 0) {
    LOG_ERROR("Header size is zero ");
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, sizeof(header), log_file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread");
  }

  CopySerializeInputBE tuple_header(header, header_size);
  tuple_record.DeserializeHeader(tuple_header);

  return true;
}

/**
 * @brief Read TupleRecordBody
 * @param schema
 * @param pool
 * @return tuple
 */
storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema, VarlenPool *pool,
                                    FILE *log_file, size_t log_file_size) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(log_file, log_file_size);
  if (body_size == 0) {
    LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), log_file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInputBE tuple_body(body, body_size);

  // We create a tuple based on the message
  storage::Tuple *tuple = new storage::Tuple(schema, true);
  tuple->DeserializeFrom(tuple_body, pool);

  return tuple;
}

/**
 * @brief Read TupleRecordBody
 * @param schema
 * @param pool
 * @return tuple
 */
void SkipTupleRecordBody(FILE *log_file, size_t log_file_size) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(log_file, log_file_size);
  if (body_size == 0) {
    LOG_ERROR("Body size is zero ");
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), log_file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInputBE tuple_body(body, body_size);
}

/**
 * @brief Read get table based on tuple record
 * @param tuple record
 * @return data table
 */
storage::DataTable *GetTable(TupleRecord &tuple_record) {
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(tuple_record.GetDatabaseOid());
  if (!db) {
    return nullptr;
  }
  assert(db);

  LOG_INFO("Table ID for this tuple: %d", (int)tuple_record.GetTableId());
  auto table = db->GetTableWithOid(tuple_record.GetTableId());
  if (!table) {
    return nullptr;
  }
  assert(table);

  return table;
}

std::string WriteAheadFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

int extract_number_from_filename(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");
  if (start_index != std::string::npos) {
    int end_index = str.find_first_not_of("0123456789", start_index);
    return atoi(str.substr(start_index, end_index - start_index).c_str());
  }
  LOG_ERROR("The last found log file doesn't have a version number.");
  return 0;
}
int ExtractNumberFromFileName(const char *name) {
  return extract_number_from_filename(name);
}

bool CompareByLogNumber(class LogFile *left, class LogFile *right) {
  return left->GetLogNumber() < right->GetLogNumber();
}

void WriteAheadFrontendLogger::InitLogFilesList() {
  struct dirent *file;
  DIR *dirp;
  cid_t temp_max_log_id_file;
  int version_number, ret_val;
  FILE *fp;

  // TODO need a better regular expression to match file name
  std::string base_name = "peloton_log_";

  LOG_INFO("Trying to read log directory");

  dirp = opendir(this->peloton_log_directory.c_str());
  if (dirp == nullptr) {
    LOG_INFO("Opendir failed: Errno: %d, error: %s", errno, strerror(errno));
  }

  while ((file = readdir(dirp)) != NULL) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // found a log file!
      LOG_INFO("Found a log file with name %s", file->d_name);

      version_number = extract_number_from_filename(file->d_name);

      fp = fopen(GetFileNameFromVersion(version_number).c_str(), "rb+");
      temp_max_log_id_file = UINT64_MAX;

      size_t read_size = fread((void *)&temp_max_log_id_file,
                               sizeof(temp_max_log_id_file), 1, fp);
      if (read_size != 1) {
        LOG_ERROR("Read from file %s failed",
                  this->GetFileNameFromVersion(version_number).c_str());
        fclose(fp);
        continue;
      }
      LOG_INFO("Got temp_max_log_id_file as %d", (int)temp_max_log_id_file);

      if (temp_max_log_id_file == 0 || temp_max_log_id_file == UINT64_MAX) {
        temp_max_log_id_file = ExtractMaxLogIdFromLogFileRecords(fp);
        LOG_INFO("ExtractMaxLogId returned %d, write it back in the file!",
                 (int)temp_max_log_id_file);

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
      }

      fclose(fp);

      // TODO update max_delimiter here in this constructor
      LogFile *new_log_file = new LogFile(NULL, file->d_name, -1,
                                          version_number, temp_max_log_id_file, 0);
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
    // @abj please follow CamelCase convention for function name :)
    // @haibinl haha gotcha :P old habits die hard :(
    int max_num = ExtractNumberFromFileName(
        this->log_files_[num_log_files - 1]->GetLogFileName().c_str());
    LOG_INFO("Got maximum log file version as %d", max_num);
    this->log_file_counter_ = ++max_num;
  } else {
    this->log_file_counter_ = 0;
  }
}

void WriteAheadFrontendLogger::CreateNewLogFile(bool close_old_file) {
  int new_file_num;
  std::string new_file_name;
  cid_t default_commit_id = 0;
  struct stat log_stats;
  int fd;

  new_file_num = log_file_counter_;

  if (close_old_file) {  // must close last opened file
    int file_list_size = this->log_files_.size();

    if (file_list_size != 0) {
      fseek(this->log_files_[file_list_size - 1]->GetFilePtr(), 0, SEEK_SET);

      fwrite((void *)&(this->max_log_id_file), sizeof(this->max_log_id_file), 1,
             this->log_files_[file_list_size - 1]->GetFilePtr());

      this->log_files_[file_list_size - 1]->SetMaxLogId(this->max_log_id_file);
      LOG_INFO("MaxLogID of the last closed file is %d",
               (int)this->max_log_id_file);

      this->max_log_id_file = 0;  // reset

      fd = fileno(this->log_files_[file_list_size - 1]->GetFilePtr());

      fstat(fd, &log_stats);
      this->log_file_size = log_stats.st_size;

      LOG_INFO("The log file to be closed has size %d",
               (int)this->log_file_size);

      this->log_files_[file_list_size - 1]->SetLogFileSize(this->log_file_size);

      fclose(this->log_files_[file_list_size - 1]->GetFilePtr());

      this->log_files_[file_list_size - 1]->SetFilePtr(nullptr);

      this->log_files_[file_list_size - 1]->SetLogFileFD(-1);  // invalidate
    }
  }

  LOG_INFO("new_file_num is %d", new_file_num);

  new_file_name = this->GetFileNameFromVersion(new_file_num);

  FILE *log_file = fopen(new_file_name.c_str(), "wb");
  if (log_file == NULL) {
    LOG_ERROR("log_file is NULL");
    return;
  }

  // TODO what if we fail here? The next file may have garbage in the header
  // now set the first 8 bytes to 0
  fwrite((void *)&default_commit_id, sizeof(default_commit_id), 1, log_file);

  this->log_file = log_file;

  int log_file_fd = fileno(log_file);

  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

  this->log_file_fd = log_file_fd;
  LOG_INFO("log_file_fd of newly created file is %d", this->log_file_fd);

  //TODO update max delimiter here in constructor
  LogFile *new_log_file_object =
      new LogFile(log_file, new_file_name, log_file_fd, new_file_num, 0, 0);

  this->log_files_.push_back(new_log_file_object);

  this->log_file_size = 0;

  log_file_counter_++;  // finally, increment log_file_counter_
  LOG_INFO("log_file_counter is %d", log_file_counter_);
}

bool WriteAheadFrontendLogger::FileSwitchCondIsTrue() {
  struct stat stat_buf;
  if (this->log_file_fd == -1) return false;

  fstat(this->log_file_fd, &stat_buf);
  this->log_file_size = stat_buf.st_size;
  return stat_buf.st_size > LOG_FILE_SWITCH_LIMIT;
}

void WriteAheadFrontendLogger::OpenNextLogFile() {
  cid_t temp_max_log_id_file;

  if (this->log_files_.size() == 0) {  // no log files, fresh start
    LOG_INFO("Size of log files list is 0.");
    this->log_file_fd = -1;
    this->log_file = NULL;
    this->log_file_size = 0;
    return;
  }

  if (this->log_file_cursor_ >= (int)this->log_files_.size()) {
    LOG_INFO("Cursor has reached the end. No more log files to read from.");
    this->log_file_fd = -1;
    this->log_file = NULL;
    this->log_file_size = 0;
    return;
  }

  if (this->log_file_cursor_ != 0)  // close old file
  {
    LOG_INFO("Closing last opened file");
    fclose(log_file);
  }

  // open the next file
  this->log_file =
      fopen(this->GetFileNameFromVersion(
                      this->log_files_[this->log_file_cursor_]->GetLogNumber())
                .c_str(),
            "rb");

  if (this->log_file == NULL) {
    LOG_ERROR("Couldn't open next log file");
    this->log_file_fd = -1;
    this->log_file = NULL;
    this->log_file_size = 0;
    return;
  } else {
    LOG_INFO("Opened new log file for recovery");
  }

  this->log_file_fd = fileno(this->log_file);
  LOG_INFO("FD of opened file is %d", (int)this->log_file_fd);

  // Skip first 8 bytes of max commit id
  size_t read_size = fread((void *)&temp_max_log_id_file,
                           sizeof(temp_max_log_id_file), 1, this->log_file);
  if (read_size != 1) {
    LOG_ERROR(
        "Read failed after opening file %s",
        this->GetFileNameFromVersion(
                  this->log_files_[this->log_file_cursor_]->GetLogNumber())
            .c_str());
  }

  LOG_INFO("On startup: MaxLogId of this file is %d",
           (int)temp_max_log_id_file);

  struct stat stat_buf;

  fstat(this->log_file_fd, &stat_buf);
  this->log_file_size = stat_buf.st_size;

  this->log_file_cursor_++;
  LOG_INFO("Cursor is now %d", (int)this->log_file_cursor_);
}

void WriteAheadFrontendLogger::TruncateLog(cid_t truncate_log_id) {
  int return_val;

  // delete stale log files except the one currently being used
  for (int i = 0; i < (int)this->log_files_.size() - 1; i++) {
    if (truncate_log_id >= this->log_files_[i]->GetMaxLogId()) {
      return_val = remove(this->log_files_[i]->GetLogFileName().c_str());
      if (return_val != 0) {
        LOG_ERROR("Couldn't delete log file: %s error: %s",
                  this->log_files_[i]->GetLogFileName().c_str(),
                  strerror(errno));
      }
      // remove entry from list anyway
      delete this->log_files_[i];
      this->log_files_.erase(this->log_files_.begin() + i);
      i--;  // update cursor
    }
  }
}

void WriteAheadFrontendLogger::InitLogDirectory() {
  int return_val;

  return_val = mkdir(this->peloton_log_directory.c_str(), 0700);
  LOG_INFO("Log directory is: %s", peloton_log_directory.c_str());

  if (return_val == 0) {
    LOG_INFO("Created Log directory successfully");
  } else if (errno == EEXIST) {
    LOG_INFO("Log Directory already exists");
  } else {
    LOG_ERROR("Creating log directory failed: %s", strerror(errno));
  }
}

void WriteAheadFrontendLogger::SetLogDirectory(char *arg
                                               __attribute__((unused))) {
  LOG_INFO("%s", arg);
}

std::string WriteAheadFrontendLogger::GetFileNameFromVersion(int version) {
  return std::string(this->peloton_log_directory.c_str()) + "/" +
         LOG_FILE_PREFIX + std::to_string(version) + LOG_FILE_SUFFIX;
}

cid_t WriteAheadFrontendLogger::ExtractMaxLogIdFromLogFileRecords(
    FILE *log_file) {
  bool reached_end_of_file = false;
  int fd;
  struct stat log_stats;
  cid_t max_log_id_so_far = 0;
  int log_file_size;

  fd = fileno(log_file);

  fstat(fd, &log_stats);
  log_file_size = log_stats.st_size;

  while (reached_end_of_file == false) {
    // Read the first byte to identify log record type
    // If that is not possible, then wrap up recovery
    auto record_type = GetNextLogRecordType(log_file, log_file_size);

    cid_t commit_id = INVALID_CID;

    TupleRecord *tuple_record;

    switch (record_type) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      case LOGRECORD_TYPE_ITERATION_DELIMITER: {
        // Check for torn log write
        TransactionRecord txn_rec(record_type);
        if (ReadTransactionRecordHeader(txn_rec, log_file, log_file_size) ==
            false) {
          return UINT64_MAX;  // TODO verify if this is correct or not
        }
        commit_id = txn_rec.GetTransactionId();
        if (commit_id > max_log_id_so_far) max_log_id_so_far = commit_id;
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        // TODO is there a memory leak here? if we fail here, tuple_record will
        // not be freed. @mperron
        if (ReadTupleRecordHeader(*tuple_record, log_file, log_file_size) ==
            false) {
          LOG_ERROR("Could not read tuple record header.");
          delete tuple_record;
          return UINT64_MAX;
        }

        auto cid = tuple_record->GetTransactionId();
        /* if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
          LOG_ERROR("Insert txd id %d not found in recovery txn table",
                    (int)cid);

          return UINT64_MAX;  // TODO verify if we should proceed even if this
                              // fails
        } */

        if (cid > max_log_id_so_far) max_log_id_so_far = cid;

        auto table = GetTable(*tuple_record);
        if (!table) {
          SkipTupleRecordBody(log_file, log_file_size);
          delete tuple_record;
          continue;
        }

        // Read off the tuple record body from the log
        ReadTupleRecordBody(table->GetSchema(), recovery_pool, log_file,
                            log_file_size);
        delete tuple_record;

        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        // TODO if this fails, is there a memory leak? @mperron
        if (ReadTupleRecordHeader(*tuple_record, log_file, log_file_size) ==
            false) {
          delete tuple_record;
          return UINT64_MAX;  // TODO same as below
        }

        auto cid = tuple_record->GetTransactionId();
        /* if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
          LOG_TRACE("Delete txd id %d not found in recovery txn table",
                    (int)cid);
          return UINT64_MAX;  // TODO verify if we should proceed even if this
                              // fails
        } */

        if (cid > max_log_id_so_far) max_log_id_so_far = cid;
        delete tuple_record;
        break;
      }
      default:
        reached_end_of_file = true;
        break;
    }
  }
  return max_log_id_so_far;
}

}  // namespace logging
}  // namespace peloton
