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

  // flush and commit all committed logs
  FlushRecords(committing_list);
  // write a committing log to file
  WriteTxnLog(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT));
  // For testing recovery, do not commit. Redo all logs in recovery.
  if (!redo_all_logs) {
    // commit all records
    CommitRecords(committing_list);
    // write a commit done log to file
    WriteTxnLog(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));
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

void PelotonFrontendLogger::FlushRecords(std::vector<txn_id_t> committing_list) {
  // flush all logs to log file
  for (txn_id_t txn_id : committing_list) {
    std::vector<TupleRecord *>* list = global_plog_pool.SearchRecordList(txn_id);
    if (list == nullptr) {
      continue;
    }

    // First, write all the record in the queue
    for (size_t i = 0; i < list->size(); i++) {
      TupleRecord *record = list->at(i);
      fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
             log_file);
    }
  }
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
  /*LogRecordList *cur = global_plog_pool->GetHeadList();
  while (cur != nullptr) {
    if (cur->IsCommitting()) {
      CommitRecords(cur);
    }
    txn_id_t txn_id = cur->GetTxnID();
    cur = cur->GetNextList();
    // don't remove tuples when doing test
    if (!suspend_committing)
      global_plog_pool->RemoveTxnLogList(txn_id);
  }
  assert(global_plog_pool->IsEmpty());
  if (max_oid != INVALID_OID) {
    auto &manager = catalog::Manager::GetInstance();
    if (max_oid > manager.GetCurrentOid())
      manager.SetNextOid(++max_oid);
  }*/
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

}  // namespace logging
}  // namespace peloton
