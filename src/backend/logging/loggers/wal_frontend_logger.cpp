
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
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/common/logger.h"

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
storage::DataTable *GetTable(TupleRecord tupleRecord);

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
    // TODO cleanup later
    this->checkpoint.Init();

    // abj1 adding code here!
    LOG_INFO("Log dir is %s", this->peloton_log_directory.c_str());
    this->InitLogDirectory();
    this->InitLogFilesList();
    this->log_file_fd = -1;   // this is a restart or a new start
    this->max_commit_id = 0;  // 0 is unused
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
  // First, write all the record in the queue
  if (global_queue.size() != 0 && this->log_file_fd == -1) {
    this->CreateNewLogFile(false);
  }

  size_t global_queue_size = global_queue.size();
  for (oid_t global_queue_itr = 0; global_queue_itr < global_queue_size;
       global_queue_itr++) {
    if (this->FileSwitchCondIsTrue()) {
      fflush_and_sync(log_file, log_file_fd, fsync_count);
      this->CreateNewLogFile(true);
    }
    auto &record = global_queue[global_queue_itr];

    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           log_file);
    LOG_INFO("TransactionID of this Log is %d",
             (int)record->GetTransactionId());
    if (record->GetTransactionId() > this->max_commit_id) {
      LOG_INFO("MaxSoFar is %d", (int)this->max_commit_id);
      this->max_commit_id = record->GetTransactionId();
    }
  }

  fflush_and_sync(log_file, log_file_fd, fsync_count);

  // Clean up the frontend logger's queue
  global_queue.clear();

  // Commit each backend logger
  {
    for (auto backend_logger : backend_loggers) {
      backend_logger->FinishedFlushing();
    }
  }
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteAheadFrontendLogger::DoRecovery() {
  cid_t start_commit_id = 0;
  if (peloton_checkpoint_mode == CHECKPOINT_TYPE_NORMAL) {
    start_commit_id = this->checkpoint.DoRecovery();
  }

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
      auto record_type =
          this->GetNextLogRecordTypeForRecovery(log_file, log_file_size);
      cid_t commit_id = INVALID_CID;
      TupleRecord *tuple_record;
      switch (record_type) {
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
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
          if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
            LOG_ERROR("Insert txd id %d not found in recovery txn table",
                      (int)cid);

            this->log_file_fd = -1;
            return;
          }

          auto table = GetTable(*tuple_record);
          if (!table || cid <= start_commit_id) {
            SkipTupleRecordBody(log_file, log_file_size);
            delete tuple_record;
            continue;
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
            CommitTransactionRecovery(commit_id);
            break;

          case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
          case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
          case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
            recovery_txn_table[tuple_record->GetTransactionId()].push_back(
                tuple_record);
            break;

          default:
            LOG_INFO("Got Type as TXN_INVALID");
            reached_end_of_file = true;
            break;
        }
      }
    }

    // Finally, abort ACTIVE transactions in recovery_txn_table
    AbortActiveTransactions();

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    if (max_oid > manager.GetNextOid()) {
      manager.SetNextOid(max_oid);
    }

    concurrency::TransactionManagerFactory::GetInstance().SetNextCid(max_cid);
  }
  this->log_file_fd = -1;
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
    LOG_ERROR("Log file is truncated");
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

LogRecordType WriteAheadFrontendLogger::GetNextLogRecordTypeForRecovery(
    FILE *log_file, size_t log_file_size) {
  char buffer;
  bool is_truncated = false;
  int ret;

  LOG_INFO("Inside GetNextLogRecordForRecovery");

  LOG_INFO("File is at position %d", (int)ftell(log_file));
  // Check if the log record type is broken
  if (IsFileTruncated(log_file, 1, log_file_size)) {
    LOG_ERROR("Log file is truncated");
    // return LOGRECORD_TYPE_INVALID;
    is_truncated = true;
  }

  // Otherwise, read the log record type
  if (!is_truncated) {
    ret = fread((void *)&buffer, 1, sizeof(char), log_file);
    if (ret <= 0) LOG_INFO("Failed an fread");
  }
  if (is_truncated || ret <= 0) {
    LOG_INFO("Call OpenNextLogFile");
    this->OpenNextLogFile();
    if (this->log_file_fd == -1) return LOGRECORD_TYPE_INVALID;

    LOG_INFO("Open succeeded. log_file_fd is %d", (int)log_file_fd);

    if (IsFileTruncated(log_file, 1, log_file_size)) {
      LOG_ERROR("Log file is truncated");
      return LOGRECORD_TYPE_INVALID;
    }
    LOG_INFO("File is not truncated.");
    ret = fread((void *)&buffer, 1, sizeof(char), log_file);
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
    LOG_ERROR("Error occured in fread ");
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
storage::DataTable *GetTable(TupleRecord tuple_record) {
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(tuple_record.GetDatabaseOid());
  if (!db) {
    return nullptr;
  }
  assert(db);

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
  txn_id_t max_commit_id;
  int version_number;
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

      fp = fopen(this->GetFileNameFromVersion(version_number).c_str(), "rb");
      max_commit_id = UINT64_MAX;

      size_t read_size =
          fread((void *)&max_commit_id, sizeof(max_commit_id), 1, fp);
      if (read_size != 1) {
        LOG_ERROR("Read from file %s failed",
                  this->GetFileNameFromVersion(version_number).c_str());
        fclose(fp);
        continue;
      }
      LOG_INFO("Got max_commit_id as %d", (int)max_commit_id);

      // TODO set max commit ID here!
      if (max_commit_id == 0 || max_commit_id == UINT64_MAX) {
        // TODO uncomment
        max_commit_id = this->ExtractMaxCommitIdFromLogFileRecords(fp);
        LOG_INFO("ExtractMaxCommitId returned %d", (int)max_commit_id);
      }

      fclose(fp);

      LogFile *new_log_file =
          new LogFile(NULL, file->d_name, -1, version_number, max_commit_id);
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
    int max_num = extract_number_from_filename(
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
  txn_id_t default_commit_id = 0;
  struct stat log_stats;
  int fd;

  new_file_num = log_file_counter_;

  if (close_old_file) {  // must close last opened file
    int file_list_size = this->log_files_.size();

    if (file_list_size != 0) {
      fseek(this->log_files_[file_list_size - 1]->GetFilePtr(), 0, SEEK_SET);

      fwrite((void *)&(this->max_commit_id), sizeof(this->max_commit_id), 1,
             this->log_files_[file_list_size - 1]->GetFilePtr());

      this->log_files_[file_list_size - 1]->SetMaxCommitId(this->max_commit_id);
      LOG_INFO("MaxCommitID of the last closed file is %d",
               (int)this->max_commit_id);

      this->max_commit_id = 0;  // reset

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
  // now set the first 4 bytes to 0
  fwrite((void *)&default_commit_id, sizeof(default_commit_id), 1, log_file);

  this->log_file = log_file;

  int log_file_fd = fileno(log_file);

  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

  this->log_file_fd = log_file_fd;
  LOG_INFO("log_file_fd of newly created file is %d", this->log_file_fd);

  LogFile *new_log_file_object =
      new LogFile(log_file, new_file_name, log_file_fd, new_file_num, 0);

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
  txn_id_t max_commit_id;

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
  size_t read_size =
      fread((void *)&max_commit_id, sizeof(max_commit_id), 1, this->log_file);
  if (read_size != 1) {
    LOG_ERROR(
        "Read failed after opening file %s",
        this->GetFileNameFromVersion(
                  this->log_files_[this->log_file_cursor_]->GetLogNumber())
            .c_str());
  }

  LOG_INFO("On startup: MaxCommitId of this file is %d", (int)max_commit_id);

  struct stat stat_buf;

  fstat(this->log_file_fd, &stat_buf);
  this->log_file_size = stat_buf.st_size;

  this->log_file_cursor_++;
  LOG_INFO("Cursor is now %d", (int)this->log_file_cursor_);
}

void WriteAheadFrontendLogger::TruncateLog(txn_id_t max_commit_id) {
  int return_val;

  // delete stale log files except the one currently being used
  for (int i = 0; i < (int)this->log_files_.size() - 1; i++) {
    if (max_commit_id >= this->log_files_[i]->GetMaxCommitId()) {
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

txn_id_t WriteAheadFrontendLogger::ExtractMaxCommitIdFromLogFileRecords(
    FILE *log_file) {
  bool reached_end_of_file = false;
  int fd;
  struct stat log_stats;
  txn_id_t max_commit_id = 0;
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
      case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
        // Check for torn log write
        TransactionRecord txn_rec(record_type);
        if (ReadTransactionRecordHeader(txn_rec, log_file, log_file_size) ==
            false) {
          return UINT64_MAX;  // TODO verify if this is correct or not
        }
        commit_id = txn_rec.GetTransactionId();
        if (commit_id > max_commit_id) max_commit_id = commit_id;
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

        if (cid > max_commit_id) max_commit_id = cid;

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

        if (cid > max_commit_id) max_commit_id = cid;
        delete tuple_record;
        break;
      }
      default:
        reached_end_of_file = true;
        break;
    }
  }
  return max_commit_id;
}

}  // namespace logging
}  // namespace peloton
