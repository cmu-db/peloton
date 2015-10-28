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
#include "backend/storage/backend_vm.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"
#include "backend/logging/loggers/peloton_backend_logger.h"

namespace peloton {
namespace logging {

LogRecordPool *PelotonFrontendLogger::global_plog_pool = nullptr;
storage::NVMBackend *PelotonFrontendLogger::backend = nullptr;

/**
 * @brief create NVM backed log pool
 */
PelotonFrontendLogger::PelotonFrontendLogger() {

  logging_type = LOGGING_TYPE_PELOTON;

  // TODO ??? try to read global_plog_pool info from a fix address
  // and recovery the lists by calling SyncLogRecordList()

  // persistent pool for all pending logs
  if (backend == nullptr) {
    backend = new storage::NVMBackend();
    assert(backend != nullptr);
  }

  if (global_plog_pool == nullptr) {
    global_plog_pool = (LogRecordPool*) backend->Allocate(sizeof(LogRecordPool));
    assert(global_plog_pool != nullptr);
    global_plog_pool->init(backend);
  } else {
    // XXX do some check?
    global_plog_pool->CheckLogRecordPool(backend);
  }
}

/**
 * @brief clean NVM space
 */
PelotonFrontendLogger::~PelotonFrontendLogger() {
  for (auto log_record : global_queue) {
    delete log_record;
  }
  global_plog_pool->Clear();
}

/**
 * @brief flush all record to the file
 */
void PelotonFrontendLogger::Flush(void) {
  //  First, write out the log record
  for (auto record : global_queue) {
    switch (record->GetType()) {
      case LOGRECORD_TYPE_INVALID:
        // do nothing
        break;
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        global_plog_pool->CreateTxnLogList(record->GetTransactionId());
        break;
      case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
        LogRecordList *txn_log_record_list = global_plog_pool->SearchRecordList(
            record->GetTransactionId());
        assert(txn_log_record_list != nullptr);
        CommitRecords(txn_log_record_list);
      }
        break;
      case LOGRECORD_TYPE_TRANSACTION_ABORT:
        // Nothing to be done for abort
        break;
      case LOGRECORD_TYPE_TRANSACTION_END:
      case LOGRECORD_TYPE_TRANSACTION_DONE:
        // don't remove tuples when doing test
        if (!suspend_committing)
          global_plog_pool->RemoveTxnLogList(record->GetTransactionId());
        break;
      default:
        // Collect the commit information
        CollectCommittedTuples((TupleRecord*) record);
        break;
    }
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

void PelotonFrontendLogger::CommitRecords(LogRecordList *txn_log_record_list) {
  // In case the commit process is interrupted
  txn_log_record_list->SetCommitting(true);
  if (suspend_committing) {
    return;
  }

  LogRecordNode *recordNode = txn_log_record_list->GetHeadNode();
  while (recordNode != NULL) {
    cid_t current_cid = INVALID_CID;
    switch (recordNode->_log_record_type) {
      case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
        current_cid = SetInsertCommitMark(recordNode->_insert_location);
        break;
      case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
        current_cid = SetDeleteCommitMark(recordNode->_delete_location);
        break;
      case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
        current_cid = SetInsertCommitMark(recordNode->_insert_location);
        SetDeleteCommitMark(recordNode->_delete_location);
        break;
      default:
        break;
    }
    if (latest_cid < current_cid) {
      latest_cid = current_cid;
    }
    recordNode = recordNode->next_node;
  }

  // All record is committed, its safe to remove them now
  txn_log_record_list->Clear();
  txn_log_record_list->SetCommitting(false);
}

void PelotonFrontendLogger::CollectCommittedTuples(TupleRecord* record) {
  if (record == nullptr) {
    return;
  }
  if (record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_INSERT
      || record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_DELETE
      || (record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE)) {
    global_plog_pool->AddLogRecord(record);
  }
}

//===--------------------------------------------------------------------===//
// Recovery 
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void PelotonFrontendLogger::DoRecovery() {
  LogRecordList *cur = global_plog_pool->GetHeadList();
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

}  // namespace logging
}  // namespace peloton
