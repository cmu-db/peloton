
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

#define LOG_FILE_SWITCH_LIMIT (1024 * 1024)

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

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

// Wrappers
storage::DataTable *GetTable(TupleRecord tupleRecord);

int ExtractNumberFromFileName(const char *name);

/**
 * @brief Open logfile and file descriptor
 */

WriteAheadFrontendLogger::WriteAheadFrontendLogger() {
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

  // TODO cleanup later
  this->checkpoint.Init();

  // abj1 adding code here!
  LOG_INFO("Log dir is %s", this->peloton_log_directory.c_str());
  this->InitLogDirectory();
  this->InitLogFilesList();
  this->log_file_fd = -1;  // this is a restart or a new start
}

/**
 * @brief close logfile
 */
WriteAheadFrontendLogger::~WriteAheadFrontendLogger() {
  for (auto log_record : global_queue) {
    delete log_record;
  }

  // close the log file
  int ret = fclose(log_file);
  if (ret != 0) {
    LOG_ERROR("Error occured while closing LogFile");
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
  for (auto record : global_queue) {
    if (this->FileSwitchCondIsTrue()) {
      fflush_and_sync(log_file, log_file_fd, fsync_count);
      this->CreateNewLogFile(true);
    }
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           log_file);
  }

  fflush_and_sync(log_file, log_file_fd, fsync_count);

  // Clean up the frontend logger's queue
  for (auto record : global_queue) {
    delete record;
  }
  global_queue.clear();

  // Commit each backend logger
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    for (auto backend_logger : backend_loggers) {
      backend_logger->Commit();
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
  if (peloton_checkpoint_mode == CHECKPOINT_TYPE_NORMAL) {
    this->checkpoint.DoRecovery();
  }

  log_file_cursor_ = 0;
  // TODO implement recovery from multiple log files
  // if (log_file_fd == -1) return;

  // Set log file size
  // log_file_size = GetLogFileSize(log_file_fd);
  // TODO handle case where we may have to recover from multiple Log files!
  this->OpenNextLogFile();

  // Go over the log size if needed
  if (log_file_size > 0) {
    bool reached_end_of_file = false;

    // Go over each log record in the log file
    while (reached_end_of_file == false) {
      // Read the first byte to identify log record type
      // If that is not possible, then wrap up recovery
      auto record_type =
          this->GetNextLogRecordTypeForRecovery(log_file, log_file_size);
      cid_t commit_id;
      TupleRecord *tuple_record;
      switch (record_type) {
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
          // Check for torn log write
          TransactionRecord txn_rec(record_type);
          if (ReadTransactionRecordHeader(txn_rec, log_file, log_file_size) ==
              false) {
            return;
          }
          commit_id = txn_rec.GetTransactionId();
          break;
        }
        case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
        case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
          tuple_record = new TupleRecord(record_type);
          // Check for torn log write
          if (ReadTupleRecordHeader(*tuple_record, log_file, log_file_size) ==
              false) {
            LOG_ERROR("Could not read tuple record header.");
            return;
          }

          auto cid = tuple_record->GetTransactionId();
          if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
            LOG_ERROR("Insert txd id %d not found in recovery txn table",
                      (int)cid);
            return;
          }

          auto table = GetTable(*tuple_record);

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
            return;
          }

          auto cid = tuple_record->GetTransactionId();
          if (recovery_txn_table.find(cid) == recovery_txn_table.end()) {
            LOG_TRACE("Delete txd id %d not found in recovery txn table",
                      (int)cid);
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
            StartTransactionRecovery(commit_id);
            break;

          case LOGRECORD_TYPE_TRANSACTION_COMMIT:
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
                       storage::Tuple *tuple) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(insert_loc.block);
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(insert_loc.block);
    tile_group = manager.GetTileGroup(insert_loc.block);
    if (max_tg < insert_loc.block) {
      max_tg = insert_loc.block;
    }
  }

  tile_group->InsertTupleFromRecovery(commit_id, insert_loc.offset, tuple);
  delete tuple;
}

void DeleteTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                       oid_t table_id, const ItemPointer &delete_loc) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(delete_loc.block);
  storage::Database *db = manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(delete_loc.block);
    tile_group = manager.GetTileGroup(delete_loc.block);
    if (max_tg < delete_loc.block) {
      max_tg = delete_loc.block;
    }
  }

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
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(remove_loc.block);
    tile_group = manager.GetTileGroup(remove_loc.block);
    if (max_tg < remove_loc.block) {
      max_tg = remove_loc.block;
    }
  }
  InsertTupleHelper(max_tg, commit_id, db_id, table_id, insert_loc, tuple);

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
  ;

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

  LOG_INFO("File is at position %d", (int)ftell(log_file));
  // Check if the log record type is broken
  if (IsFileTruncated(log_file, 1, log_file_size)) {
    LOG_ERROR("Log file is truncated");
    return LOGRECORD_TYPE_INVALID;
  }

  LOG_INFO("Inside GetNextLogRecordForRecovery");
  // Otherwise, read the log record type
  int ret = fread((void *)&buffer, 1, sizeof(char), log_file);
  if (ret <= 0) {
    LOG_INFO("Failed an fread. Call OpenNextLogFile");
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
 * @brief Read get table based on tuple record
 * @param tuple record
 * @return data table
 */
storage::DataTable *GetTable(TupleRecord tuple_record) {
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(tuple_record.GetDatabaseOid());
  assert(db);

  auto table = db->GetTableWithOid(tuple_record.GetTableId());
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
  // TODO need a better regular expression to match file name
  std::string base_name = "peloton_log_";

  LOG_INFO("Trying to read log directory");

  dirp = opendir(this->peloton_log_directory.c_str());
  if (dirp == nullptr) {
    LOG_INFO("Opendir failed: Errno: %d, error: %s", errno, strerror(errno));
  }
  /*for (int i = 0; i < n; i++) {
    if (strncmp(list[i]->d_name, base_name.c_str(), base_name.length()) == 0) {
      LOG_INFO("Found a log file with name %s", list[i]->d_name);
      LogFile *new_log_file = new LogFile(NULL, list[i]->d_name, -1);
      this->log_files_.push_back(new_log_file);
    }*/

  while ((file = readdir(dirp)) != NULL) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // found a log file!
      LOG_INFO("Found a log file with name %s", file->d_name);

      LogFile *new_log_file = new LogFile(
          NULL, file->d_name, -1, extract_number_from_filename(file->d_name));
      this->log_files_.push_back(new_log_file);
    }
  }
  closedir(dirp);

  int num_log_files;
  num_log_files = this->log_files_.size();

  LOG_INFO("The number of log files found: %d", num_log_files);

  std::sort(this->log_files_.begin(), this->log_files_.end(),
            CompareByLogNumber);

  /*LOG_INFO("After sorting, the list of log files looks like this:");

  for (int i = 0; i < num_log_files; i++) {
    LOG_INFO("Name: %s, Number: %d, Size: %d, FD: %d",
  this->log_files_[i]->log_file_name_.c_str(), this->log_files_[i]->log_number_,
  this->log_files_[i]->log_file_size_, this->log_files_[i]->log_file_fd_);
  }*/

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
  int new_file_num = log_file_counter_;
  LOG_INFO("new_file_num is %d", new_file_num);
  std::string new_file_name;

  new_file_name = this->GetFileNameFromVersion(new_file_num);

  FILE *log_file = fopen(new_file_name.c_str(), "wb");
  if (log_file == NULL) {
    LOG_ERROR("log_file is NULL");
  }

  this->log_file = log_file;

  int log_file_fd = fileno(log_file);

  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

  this->log_file_fd = log_file_fd;
  LOG_INFO("log_file_fd of newly created file is %d", this->log_file_fd);

  LogFile *new_log_file_object =
      new LogFile(log_file, new_file_name, log_file_fd, new_file_num);

  if (close_old_file) {  // must close last opened file
    int file_list_size = this->log_files_.size();

    if (file_list_size != 0) {
      this->log_files_[file_list_size - 1]->SetLogFileSize(this->log_file_size);
      fclose(this->log_files_[file_list_size - 1]->GetFilePtr());
      this->log_files_[file_list_size - 1]->SetLogFileFD(-1);  // invalidate
      // TODO set max commit_id here!
    }
  }

  this->log_files_.push_back(new_log_file_object);

  log_file_counter_++;  // finally, increment log_file_counter_
  LOG_INFO("log_file_counter is %d", log_file_counter_);
}

bool WriteAheadFrontendLogger::FileSwitchCondIsTrue() {
  struct stat stat_buf;
  if (this->log_file_fd == -1) return false;

  fstat(this->log_file_fd, &stat_buf);
  return stat_buf.st_size > LOG_FILE_SWITCH_LIMIT;
}

void WriteAheadFrontendLogger::OpenNextLogFile() {
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

  struct stat stat_buf;

  fstat(this->log_file_fd, &stat_buf);
  this->log_file_size = stat_buf.st_size;

  this->log_file_cursor_++;
  LOG_INFO("Cursor is now %d", (int)this->log_file_cursor_);
}

void WriteAheadFrontendLogger::TruncateLog(int max_commit_id) {
  int return_val;

  // delete stale log files except the one currently being used
  for (int i = 0; i < (int)this->log_files_.size() - 1; i++) {
    if (max_commit_id >= this->log_files_[i]->GetMaxCommitId()) {
      return_val = remove(this->log_files_[i]->GetLogFileName().c_str());
      if (return_val != 0) {
        LOG_ERROR("Couldn't delete log file: %s",
                  this->log_files_[i]->GetLogFileName().c_str());
      }
      // remove entry from list anyway
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
  } else if (return_val == EEXIST) {
    LOG_INFO("Log Directory already exists");
  } else {
    LOG_ERROR("Creating log directory failed: %s", strerror(errno));
  }
}

void WriteAheadFrontendLogger::SetLogDirectory(char *arg) {
  LOG_INFO("%s", arg);
}

std::string WriteAheadFrontendLogger::GetFileNameFromVersion(int version) {
  return std::string(this->peloton_log_directory.c_str()) + "/" +
         LOG_FILE_PREFIX + std::to_string(version) + LOG_FILE_SUFFIX;
}

}  // namespace logging
}  // namespace peloton
