/*-------------------------------------------------------------------------
 *
 * wal_frontend_logger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wal_frontend_logger.cpp
 *
 *-------------------------------------------------------------------------
 */


#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/pool.h"
#include "backend/concurrency/transaction.h"
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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

size_t GetLogFileSize(int log_file_fd);

bool IsFileTruncated(FILE *log_file, size_t size_to_read, size_t log_file_size);

size_t GetNextFrameSize(FILE *log_file, size_t log_file_size);

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

bool ReadTransactionRecordHeader(TransactionRecord &txn_record, FILE *log_file,
                                 size_t log_file_size);

bool ReadTupleRecordHeader(TupleRecord &tuple_record, FILE *log_file,
                           size_t log_file_size);

storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema, VarlenPool *pool,
                                    FILE *log_file, size_t log_file_size);

// Wrappers
storage::DataTable *GetTable(TupleRecord tupleRecord);

/**
 * @brief Open logfile and file descriptor
 */
WriteAheadFrontendLogger::WriteAheadFrontendLogger() {
  logging_type = LOGGING_TYPE_DRAM_NVM;

  LOG_INFO("Log File Name :: %s", GetLogFileName().c_str());

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
  }

  // allocate pool
  recovery_pool = new VarlenPool(BACKEND_TYPE_MM);
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

/**
 * @brief flush all the log records to the file
 */
void WriteAheadFrontendLogger::FlushLogRecords(void) {
  // First, write all the record in the queue
  for (auto record : global_queue) {
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           log_file);
  }

  // Then, flush
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
  // Set log file size
  log_file_size = GetLogFileSize(log_file_fd);

  // Go over the log size if needed
  if (log_file_size > 0) {
    bool reached_end_of_file = false;

    // Start the recovery transaction
    auto &txn_manager = concurrency::TransactionManager::GetInstance();

    // Although we call BeginTransaction here, recovery txn will not be
    // recoreded in log file since we are in recovery mode
    auto recovery_txn = txn_manager.BeginTransaction();

    // Go over each log record in the log file
    while (reached_end_of_file == false) {
      // Read the first byte to identify log record type
      // If that is not possible, then wrap up recovery
      auto record_type = GetNextLogRecordType(log_file, log_file_size);

      switch (record_type) {
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          AddTransactionToRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_END:
          RemoveTransactionFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
          MoveCommittedTuplesToRecoveryTxn(recovery_txn);
          break;

        case LOGRECORD_TYPE_TRANSACTION_ABORT:
          AbortTuplesFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
          InsertTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
          DeleteTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
          UpdateTuple(recovery_txn);
          break;

        default:
          reached_end_of_file = true;
          break;
      }
    }

    // Commit the recovery transaction
    txn_manager.CommitTransaction();

    // Finally, abort ACTIVE transactions in recovery_txn_table
    AbortActiveTransactions();

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);
  }
}

/**
 * @brief Add new txn to recovery table
 */
void WriteAheadFrontendLogger::AddTransactionToRecoveryTable() {
  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_BEGIN);

  // Check for torn log write
  if (ReadTransactionRecordHeader(txn_record, log_file, log_file_size) ==
      false) {
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

  // create the new txn object and added it into recovery recovery_txn_table
  concurrency::Transaction *txn =
      new concurrency::Transaction(txn_id, INVALID_CID);
  recovery_txn_table.insert(std::make_pair(txn_id, txn));
  LOG_TRACE("Added txd id %d object in table", (int)txn_id);
}

/**
 * @brief Remove txn from recovery table
 */
void WriteAheadFrontendLogger::RemoveTransactionFromRecoveryTable() {
  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_END);

  // Check for torn log write
  if (ReadTransactionRecordHeader(txn_record, log_file, log_file_size) ==
      false) {
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

  // remove txn from recovery txn table
  if (recovery_txn_table.find(txn_id) != recovery_txn_table.end()) {
    auto txn = recovery_txn_table.at(txn_id);
    recovery_txn_table.erase(txn_id);

    // drop the txn as well
    delete txn;
    LOG_TRACE("Erase txd id %d object in table", (int)txn_id);
  } else {
    LOG_TRACE("Erase txd id %d not found in recovery txn table", (int)txn_id);
  }
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit
 * them later
 * @param recovery txn
 */
void WriteAheadFrontendLogger::MoveCommittedTuplesToRecoveryTxn(
    concurrency::Transaction *recovery_txn) {
  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_COMMIT);

  // Check for torn log write
  if (ReadTransactionRecordHeader(txn_record, log_file, log_file_size) ==
      false) {
    return;
  }

  // Get info about the transaction from recovery table
  auto txn_id = txn_record.GetTransactionId();
  if (recovery_txn_table.find(txn_id) != recovery_txn_table.end()) {
    auto txn = recovery_txn_table.at(txn_id);
    // Copy inserted/deleted tuples to recovery transaction
    MoveTuples(recovery_txn, txn);

    LOG_TRACE("Commit txd id %d object in table", (int)txn_id);
  } else {
    LOG_TRACE("Commit txd id %d not found in recovery txn table", (int)txn_id);
  }
}

/**
 * @brief Move Tuples from log record-based local transaction to recovery
 * transaction
 * @param destination
 * @param source
 */
void WriteAheadFrontendLogger::MoveTuples(concurrency::Transaction *destination,
                                     concurrency::Transaction *source) {
  // This is the local transaction
  auto inserted_tuples = source->GetInsertedTuples();
  // Record the inserts in recovery txn
  for (auto entry : inserted_tuples) {
    oid_t tile_group_id = entry.first;
    for (auto tuple_slot : entry.second) {
      destination->RecordInsert(ItemPointer(tile_group_id, tuple_slot));
    }
  }

  // Record the deletes in recovery txn
  auto deleted_tuples = source->GetDeletedTuples();
  for (auto entry : deleted_tuples) {
    oid_t tile_group_id = entry.first;
    for (auto tuple_slot : entry.second) {
      destination->RecordDelete(ItemPointer(tile_group_id, tuple_slot));
    }
  }

  // Clear inserted/deleted tuples from txn, just in case
  source->ResetState();
}

/**
 * @brief abort tuple
 */
void WriteAheadFrontendLogger::AbortTuplesFromRecoveryTable() {
  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_ABORT);

  // Check for torn log write
  if (ReadTransactionRecordHeader(txn_record, log_file, log_file_size) ==
      false) {
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

  // Get info about the transaction from recovery table
  if (recovery_txn_table.find(txn_id) != recovery_txn_table.end()) {
    auto txn = recovery_txn_table.at(txn_id);
    AbortTuples(txn);
    LOG_INFO("Abort txd id %d object in table", (int)txn_id);
  } else {
    LOG_INFO("Abort txd id %d not found in recovery txn table", (int)txn_id);
  }
}

/**
 * @brief Abort tuples inside txn
 * @param txn
 */
void WriteAheadFrontendLogger::AbortTuples(concurrency::Transaction *txn) {
  LOG_INFO("Abort txd id %d object in table", (int)txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  // Record the aborted inserts in recovery txn
  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    oid_t tile_group_id = entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);

    for (auto tuple_slot : entry.second) {
      tile_group.get()->AbortInsertedTuple(tuple_slot);
    }
  }

  // Record the aborted deletes in recovery txn
  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : txn->GetDeletedTuples()) {
    oid_t tile_group_id = entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);

    for (auto tuple_slot : entry.second) {
      tile_group.get()->AbortDeletedTuple(tuple_slot, txn->GetTransactionId());
    }
  }

  // Clear inserted/deleted tuples from txn, just in case
  txn->ResetState();
}

/**
 * @brief Abort tuples inside txn table
 */
void WriteAheadFrontendLogger::AbortActiveTransactions() {
  // Clean up the recovery table to ignore active transactions
  for (auto active_txn_entry : recovery_txn_table) {
    // clean up the transaction
    auto active_txn = active_txn_entry.second;
    AbortTuples(active_txn);
    delete active_txn;
  }

  recovery_txn_table.clear();
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void WriteAheadFrontendLogger::InsertTuple(concurrency::Transaction *recovery_txn) {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  // Check for torn log write
  if (ReadTupleRecordHeader(tuple_record, log_file, log_file_size) == false) {
    LOG_ERROR("Could not read tuple record header.");
    return;
  }

  auto txn_id = tuple_record.GetTransactionId();
  if (recovery_txn_table.find(txn_id) == recovery_txn_table.end()) {
    LOG_ERROR("Insert txd id %d not found in recovery txn table", (int)txn_id);
    return;
  }

  auto table = GetTable(tuple_record);

  // Read off the tuple record body from the log
  auto tuple = ReadTupleRecordBody(table->GetSchema(), recovery_pool, log_file,
                                   log_file_size);

  // Check for torn log write
  if (tuple == nullptr) {
    return;
  }

  auto target_location = tuple_record.GetInsertLocation();
  auto tile_group_id = target_location.block;
  auto tuple_slot = target_location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);

  auto txn = recovery_txn_table.at(txn_id);

  // Create new tile group if table doesn't already have that tile group
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(tile_group_id);
    tile_group = manager.GetTileGroup(tile_group_id);
    if (max_oid < tile_group_id) {
      max_oid = tile_group_id;
    }
  }

  // Do the insert !
  auto inserted_tuple_slot = tile_group->InsertTuple(
      recovery_txn->GetTransactionId(), tuple_slot, tuple);

  if (inserted_tuple_slot == INVALID_OID) {
    // TODO: We need to abort on failure !
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  } else {
    txn->RecordInsert(target_location);
    table->IncreaseNumberOfTuplesBy(1);
  }

  delete tuple;
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void WriteAheadFrontendLogger::DeleteTuple(concurrency::Transaction *recovery_txn) {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_DELETE);

  // Check for torn log write
  if (ReadTupleRecordHeader(tuple_record, log_file, log_file_size) == false) {
    return;
  }

  auto txn_id = tuple_record.GetTransactionId();
  if (recovery_txn_table.find(txn_id) == recovery_txn_table.end()) {
    LOG_TRACE("Delete txd id %d not found in recovery txn table", (int)txn_id);
    return;
  }

  auto table = GetTable(tuple_record);

  ItemPointer delete_location = tuple_record.GetDeleteLocation();

  // Try to delete the tuple
  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    // TODO: We need to abort on failure !
    recovery_txn->SetResult(Result::RESULT_FAILURE);
    return;
  }

  auto txn = recovery_txn_table.at(txn_id);
  txn->RecordDelete(delete_location);
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void WriteAheadFrontendLogger::UpdateTuple(concurrency::Transaction *recovery_txn) {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_UPDATE);

  // Check for torn log write
  if (ReadTupleRecordHeader(tuple_record, log_file, log_file_size) == false) {
    return;
  }

  auto txn_id = tuple_record.GetTransactionId();
  if (recovery_txn_table.find(txn_id) == recovery_txn_table.end()) {
    LOG_TRACE("Update txd id %d not found in recovery txn table", (int)txn_id);
    return;
  }

  auto txn = recovery_txn_table.at(txn_id);

  auto table = GetTable(tuple_record);

  auto tuple = ReadTupleRecordBody(table->GetSchema(), recovery_pool, log_file,
                                   log_file_size);

  // Check for torn log write
  if (tuple == nullptr) {
    return;
  }

  // First, redo the delete
  ItemPointer delete_location = tuple_record.GetDeleteLocation();

  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  } else {
    txn->RecordDelete(delete_location);

    auto target_location = tuple_record.GetInsertLocation();
    auto tile_group_id = target_location.block;
    auto tuple_slot = target_location.offset;
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tile_group_id);

    // Create new tile group if table doesn't already have that tile group
    if (tile_group == nullptr) {
      table->AddTileGroupWithOid(tile_group_id);
      tile_group = manager.GetTileGroup(tile_group_id);
      if (max_oid < tile_group_id) {
        max_oid = tile_group_id;
      }
    }

    // Do the insert !
    auto inserted_tuple_slot = tile_group->InsertTuple(
        recovery_txn->GetTransactionId(), tuple_slot, tuple);
    if (inserted_tuple_slot == INVALID_OID) {
      recovery_txn->SetResult(Result::RESULT_FAILURE);
    } else {
      txn->RecordInsert(target_location);
    }
  }

  delete tuple;
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

}  // namespace logging
}  // namespace peloton
