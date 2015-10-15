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

/**
 * @brief create NVM backed log pool
 */
PelotonFrontendLogger::PelotonFrontendLogger() {

  logging_type = LOGGING_TYPE_PELOTON;

  // persistent pool for all pending logs
  backend = new storage::NVMBackend();

  // TODO ??? try to read global_plog_pool info from a fix address
  // and recovery the lists by calling SyncLogRecordList()
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
  delete backend;
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
      case LOGRECORD_TYPE_TRANSACTION_END:
      case LOGRECORD_TYPE_TRANSACTION_DONE:
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
  backend_loggers = GetBackendLoggers();

  for (auto backend_logger : backend_loggers) {
    if (backend_logger->IsWaitingForFlushing()) {
      backend_logger->Commit();
    }
  }
}

void PelotonFrontendLogger::CommitRecords(LogRecordList *txn_log_record_list) {
  // In case the commit process is interrupted
  txn_log_record_list->SetCommitting(true);

  LogRecordNode *recordNode = txn_log_record_list->GetHeadNode();

  while (recordNode != NULL) {
    cid_t current_cid = INVALID_CID;
    switch (recordNode->_log_record_type) {
      case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
        current_cid = SetInsertCommitMark(recordNode->_insert_location, true);
        break;
      case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
        current_cid = SetDeleteCommitMark(recordNode->_delete_location, true);
        break;
      case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
        current_cid = SetInsertCommitMark(recordNode->_insert_location, true);
        SetDeleteCommitMark(recordNode->_delete_location, true);
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
  while (!global_plog_pool->IsEmpty()) {
    LogRecordList *cur = global_plog_pool->GetHeadList();
    if (cur->IsCommitting()) {
      CommitRecords(cur);
    }
    global_plog_pool->RemoveTxnLogList(cur->GetTxnID());
  }
  assert(global_plog_pool->IsEmpty());
  if (max_oid != INVALID_OID) {
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);
  }
  // TODO reset transaction manager with latest cid and txn id
}

cid_t PelotonFrontendLogger::SetInsertCommitMark(ItemPointer location,
                                                bool commit) {
  //Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetInsertCommit(location.offset, commit);
  tile_group_header->IncrementActiveTupleCount();
  LOG_INFO("<%p, %u> : slot is insert committed", tile_group, location.offset);
  if( max_oid < location.block ){
    max_oid = location.block;
  }
  // TODO sync change
  return tile_group_header->GetBeginCommitId(location.offset);
}

cid_t PelotonFrontendLogger::SetDeleteCommitMark(ItemPointer location,
                                                bool commit) {
  // Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetDeleteCommit(location.offset, commit);
  tile_group_header->DecrementActiveTupleCount();
  LOG_INFO("<%p, %u> : slot is delete committed", tile_group, location.offset);
  if( max_oid < location.block ){
    max_oid = location.block;
  }
  // TODO sync change
  return tile_group_header->GetEndCommitId(location.offset);
}

}  // namespace logging
}  // namespace peloton
