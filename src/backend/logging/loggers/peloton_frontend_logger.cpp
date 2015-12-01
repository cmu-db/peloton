/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"
#include "backend/logging/loggers/peloton_backend_logger.h"

namespace peloton {
namespace logging {

size_t GetLogFileSize(int log_file_fd);

LogRecordType GetNextLogRecordType(FILE *log_file);

bool ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                 FILE *log_file);

bool ReadTupleRecordHeader(TupleRecord& tuple_record,
                           FILE *log_file);

/**
 * @brief create NVM backed log pool
 */
PelotonFrontendLogger::PelotonFrontendLogger() {

  logging_type = LOGGING_TYPE_PELOTON;

  // open log file and file descriptor
  // we open it in append + binary mode
  log_file = fopen(GetLogFileName().c_str(),"ab+");
  if(log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if( log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }
}

/**
 * @brief clean NVM space
 */
PelotonFrontendLogger::~PelotonFrontendLogger() {
  for (auto log_record : global_queue) {
    delete log_record;
  }
  global_plog_pool.Clear();
}

/**
 * @brief flush all record to the file
 */
void PelotonFrontendLogger::Flush(void) {
  // process the log records
  std::vector<txn_id_t> committing_list;
  std::vector<txn_id_t> deleting_list;
  for (auto record : global_queue) {
    bool deleteRecord = true;
    switch (record->GetType()) {
      case LOGRECORD_TYPE_INVALID:
        // do nothing
        break;
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        global_plog_pool.CreateTxnLogList(record->GetTransactionId());
        break;
      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        committing_list.push_back(record->GetTransactionId());
        break;
      case LOGRECORD_TYPE_TRANSACTION_ABORT:
        // Nothing to be done for abort
        break;
      case LOGRECORD_TYPE_TRANSACTION_END:
      case LOGRECORD_TYPE_TRANSACTION_DONE:
        // if a txn is not committed, logs will be removed here
        // Note that list should not be removed immediately, it should be removed after flush and commit.
        deleting_list.push_back(record->GetTransactionId());
        break;
      default:
        // Collect the commit information, don't delete record if CollectCommittedTuples is done
        deleteRecord = !CollectCommittedTuples((TupleRecord*) record);
        break;
    }
    if (deleteRecord) {
      delete record;
    }
  }
  global_queue.clear();

  if (!committing_list.empty()) {
    // flush and commit all committed logs
    size_t flush_count = FlushRecords(committing_list);
    // write a committing log to file
    // piggyback the number of committing logs count as txn_id in this log
    WriteTxnLog(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, flush_count));
    // For testing recovery, do not commit. Redo all logs in recovery.
    if (!redo_all_logs) {
      // commit all records
      CommitRecords(committing_list);
      // write a commit done log to file
      WriteTxnLog(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));
    }
  }
  // remove any finished txn logs
  for (txn_id_t txn_id : deleting_list) {
    global_plog_pool.RemoveTxnLogList(txn_id);
  }

  // Commit each backend logger 
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    for (auto backend_logger : backend_loggers) {
      backend_logger->Commit();
    }
  }
}

void PelotonFrontendLogger::WriteTxnLog(TransactionRecord txnLog) {
  txnLog.Serialize(output_buffer);
  fwrite(txnLog.GetMessage(), sizeof(char), txnLog.GetMessageLength(),
               log_file);
  // Then, flush
  int ret = fflush(log_file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(log_file_fd);
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}

size_t PelotonFrontendLogger::FlushRecords(std::vector<txn_id_t> committing_list) {
  // flush all logs to log file
  size_t flush_count = 0;
  for (txn_id_t txn_id : committing_list) {
    std::vector<TupleRecord *>* list = global_plog_pool.SearchRecordList(txn_id);
    if (list == nullptr) {
      continue;
    }
    flush_count += list->size();
    // First, write all the record in the queue
    for (size_t i = 0; i < list->size(); i++) {
      TupleRecord *record = list->at(i);
      fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(), log_file);
    }
  }
  return flush_count;
  // No need to flush now, will flush in WriteTxnLog
}

void PelotonFrontendLogger::CommitRecords(std::vector<txn_id_t> committing_list) {
  // flip commit marks
  for (txn_id_t txn_id : committing_list) {
    std::vector<TupleRecord *>* list = global_plog_pool.SearchRecordList(txn_id);
    if (list == nullptr) {
      continue;
    }
    for (size_t i = 0; i < list->size(); i++) {
      TupleRecord *record = list->at(i);
      cid_t current_cid = INVALID_CID;
      switch (record->GetType()) {
        case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
          current_cid = SetInsertCommitMark(record->GetInsertLocation());
          break;
        case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
          current_cid = SetDeleteCommitMark(record->GetDeleteLocation());
          break;
        case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
          current_cid = SetInsertCommitMark(record->GetInsertLocation());
          SetDeleteCommitMark(record->GetDeleteLocation());
          break;
        default:
          break;
      }
      if (latest_cid < current_cid) {
        latest_cid = current_cid;
      }
    }
    // All record is committed, its safe to remove them now
    global_plog_pool.RemoveTxnLogList(txn_id);
  }
}

bool PelotonFrontendLogger::CollectCommittedTuples(TupleRecord* record) {
  if (record == nullptr) {
    return false;
  }
  if (record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_INSERT
      || record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_DELETE
      || (record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE)) {
    return global_plog_pool.AddLogRecord(record) == 0;
  } else {
    return false;
  }
}

//===--------------------------------------------------------------------===//
// Recovery 
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void PelotonFrontendLogger::DoRecovery() {
  // Go over the log size if needed
  if (GetLogFileSize(log_file_fd) > 0) {
    bool reached_end_of_file = false;
    // check whether first item is LOGRECORD_TYPE_TRANSACTION_COMMIT
    // if not, no need to recover.
    // if yes, need to recovery all logs before we hit LOGRECORD_TYPE_TRANSACTION_DONE
    if (DoNeedRecovery()) {
      TransactionRecord dummy_record(LOGRECORD_TYPE_INVALID);
      cid_t current_cid = INVALID_CID;
      // Go over each log record in the log file
      while (!reached_end_of_file) {
        // Read the first byte to identify log record type
        // If that is not possible, then wrap up recovery
        LogRecordType logType = GetNextLogRecordType(log_file);
        switch (logType) {
          case LOGRECORD_TYPE_TRANSACTION_DONE:
          case LOGRECORD_TYPE_TRANSACTION_COMMIT:
            // read but do nothing
            ReadTransactionRecordHeader(dummy_record, log_file);
            break;
          case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
          {
            TupleRecord insert_record(LOGRECORD_TYPE_PELOTON_TUPLE_INSERT);
            ReadTupleRecordHeader(insert_record, log_file);
            current_cid = SetInsertCommitMark(insert_record.GetInsertLocation());
          }
            break;
          case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
          {
            TupleRecord delete_record(LOGRECORD_TYPE_PELOTON_TUPLE_DELETE);
            ReadTupleRecordHeader(delete_record, log_file);
            current_cid = SetDeleteCommitMark(delete_record.GetDeleteLocation());
          }
            break;
          case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
          {
            TupleRecord update_record(LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE);
            ReadTupleRecordHeader(update_record, log_file);
            current_cid = SetInsertCommitMark(update_record.GetInsertLocation());
            SetDeleteCommitMark(update_record.GetDeleteLocation());
          }
            break;
          default:
            reached_end_of_file = true;
            break;
        }
      }
      if (latest_cid < current_cid) {
        latest_cid = current_cid;
      }
      // write a commit done log to file, not redo next time
      WriteTxnLog(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));
    }
    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);
  }
  // TODO How to reset transaction manager with latest cid, if no item is recovered
}

cid_t PelotonFrontendLogger::SetInsertCommitMark(ItemPointer location) {
  // XXX Do we need to lock tile before operating?
  // Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  if (!tile_group_header->GetInsertCommit(location.offset)) {
    tile_group_header->SetInsertCommit(location.offset, true);
  }
  LOG_INFO("<%p, %lu> : slot is insert committed", tile_group, location.offset);
  if( max_oid < location.block ){
    max_oid = location.block;
  }
  // TODO sync change
  return tile_group_header->GetBeginCommitId(location.offset);
}

cid_t PelotonFrontendLogger::SetDeleteCommitMark(ItemPointer location) {
  // XXX Do we need to lock tile before operating?
  // Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  if (!tile_group_header->GetDeleteCommit(location.offset)) {
    tile_group_header->SetDeleteCommit(location.offset, true);
  }
  LOG_INFO("<%p, %lu> : slot is delete committed", tile_group, location.offset);
  if( max_oid < location.block ){
    max_oid = location.block;
  }
  // TODO sync change
  return tile_group_header->GetEndCommitId(location.offset);
}

std::string PelotonFrontendLogger::GetLogFileName(void) {
  auto& log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

// Check whether need to recovery, if yes, reset fseek to the right place.
bool PelotonFrontendLogger::DoNeedRecovery(void) {
  if (redo_all_logs) {
    return true;
  }
  // Read the last record type
  fseek(log_file, -TransactionRecord::GetTransactionRecordSize(), SEEK_END);

  auto log_record_type = GetNextLogRecordType(log_file);

  // If the previous run break
  if( log_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT){
    TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_COMMIT);
    // read the last commit transaction log
    if( ReadTransactionRecordHeader(txn_record, log_file) == false ) {
      return false;
    }
    // get tuple log count from txn_id
    size_t tuple_log_count = txn_record.GetTransactionId();
    // Peloton log items have fixed size.
    size_t rollback_size = tuple_log_count * TupleRecord::GetTupleRecordSize()
                                    + TransactionRecord::GetTransactionRecordSize();
    fseek(log_file, -rollback_size, SEEK_END);
    return true;
  } else {
    return false;
  }
}

}  // namespace logging
}  // namespace peloton
