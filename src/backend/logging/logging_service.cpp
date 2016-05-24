//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pelton_service.h
//
// Identification: src/backend/networking/pelton_service.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging_service.h"
#include "log_manager.h"
#include "logging_util.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"

#include "backend/concurrency/transaction_manager_factory.h"

#include <iostream>

namespace peloton {
namespace logging {

LoggingService::LoggingService() {
  recovery_pool = new VarlenPool(BACKEND_TYPE_MM);
  // we will sync manually so turn this off for now
  LogManager::GetInstance().SetSyncCommit(false);
  replication_sequence_number_.store(1);
}

LogRecordType GetRecordTypeFromBytes(char *&workingPointer) {
  CopySerializeInputBE input(workingPointer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  workingPointer++;
  return log_record_type;
}

size_t GetHeaderLen(char *&workingPointer) {
  size_t frame_size;

  // Read next 4 bytes as an integer
  CopySerializeInputBE frameCheck(workingPointer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt()) + sizeof(int32_t);
  return frame_size;
}

void GetTransactionRecord(TransactionRecord &record, char *&workingPointer) {
  auto frame_size = GetHeaderLen(workingPointer);

  CopySerializeInputBE txn_header(workingPointer, frame_size);
  record.Deserialize(txn_header);
  workingPointer += frame_size;
}

void GetTupleRecordHeader(TupleRecord &record, char *&workingPointer) {
  auto frame_size = GetHeaderLen(workingPointer);

  CopySerializeInputBE txn_header(workingPointer, frame_size);
  record.DeserializeHeader(txn_header);
  workingPointer += frame_size;
}

storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema, VarlenPool *pool,
                                    char *&workingPointer) {
  // Check if the frame is broken
  size_t body_size = GetHeaderLen(workingPointer);
  if (body_size == 0) {
    LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  CopySerializeInputBE tuple_body(workingPointer, body_size);

  // We create a tuple based on the message
  storage::Tuple *tuple = new storage::Tuple(schema, true);
  tuple->DeserializeFrom(tuple_body, pool);
  workingPointer += body_size;

  return tuple;
}

/**
 * @brief Add new txn to recovery table
 */
void LoggingService::StartTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord *> tuple_recs;
  recovery_txn_table[commit_id] = tuple_recs;
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit
 * them later
 * @param recovery txn
 */
void LoggingService::CommitTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord *> &tuple_records = recovery_txn_table[commit_id];
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();
  for (auto it = tuple_records.begin(); it != tuple_records.end(); it++) {
    TupleRecord *curr = *it;
    switch (curr->GetType()) {
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
        InsertTuple(curr, txn_manager);
        break;
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
      case LOGRECORD_TYPE_WBL_TUPLE_UPDATE:
        UpdateTuple(curr, txn_manager);
        break;
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
      case LOGRECORD_TYPE_WBL_TUPLE_DELETE:
        DeleteTuple(curr, txn_manager);
        break;
      default:
        continue;
    }
    delete curr;
  }
  txn_manager.CommitTransaction();
  max_cid = commit_id + 1;
}

extern void InsertTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                              oid_t table_id, const ItemPointer &insert_loc,
                              storage::Tuple *tuple,
                              bool should_increase_tuple_count = true);

extern void DeleteTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                              oid_t table_id, const ItemPointer &delete_loc);

extern void UpdateTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id,
                              oid_t table_id, const ItemPointer &remove_loc,
                              const ItemPointer &insert_loc,
                              storage::Tuple *tuple);

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void LoggingService::InsertTuple(TupleRecord *record,
                                 concurrency::TransactionManager &txn_manager) {
  InsertTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetInsertLocation(), record->GetTuple());
  txn_manager.PerformInsert(record->GetInsertLocation());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void LoggingService::DeleteTuple(TupleRecord *record,
                                 concurrency::TransactionManager &txn_manager) {
  DeleteTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetDeleteLocation());
  txn_manager.PerformDelete(record->GetDeleteLocation());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */

void LoggingService::UpdateTuple(TupleRecord *record,
                                 concurrency::TransactionManager &txn_manager) {
  UpdateTupleHelper(max_oid, record->GetTransactionId(),
                    record->GetDatabaseOid(), record->GetTableId(),
                    record->GetDeleteLocation(), record->GetInsertLocation(),
                    record->GetTuple());
  txn_manager.PerformUpdate(record->GetDeleteLocation(),
                            record->GetInsertLocation());
}
// implements LoggingService ------------------------------------------

void LoggingService::LogRecordReplay(
    __attribute__((unused))::google::protobuf::RpcController *controller,
    __attribute__((unused)) const networking::LogRecordReplayRequest *request,
    __attribute__((unused)) networking::LogRecordReplayResponse *response,
    __attribute__((unused))::google::protobuf::Closure *done) {
  LogManager &manager = LogManager::GetInstance();
  if (request == nullptr) {
    manager.GetFrontendLogger(0)->RemoteDone(response->sequence_number());
    return;
  }
  long curr_seq = request->sequence_number();
  while (replication_sequence_number_.load() != curr_seq)
    ;

  const char *messages = request->log().c_str();
  auto size = request->log().size();
  bool wait_for_sync = request->sync_type() == networking::SYNC;
  char *workingPointer = (char *)messages;
  // Go over each log record in the log file
  while (workingPointer < messages + size) {
    // Read the first byte to identify log record type
    // If that is not possible, then wrap up recovery
    auto record_type = GetRecordTypeFromBytes(workingPointer);
    cid_t log_id = INVALID_CID;
    TupleRecord *tuple_record;

    switch (record_type) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      case LOGRECORD_TYPE_ITERATION_DELIMITER: {
        // Check for torn log write
        TransactionRecord txn_rec(record_type);
        GetTransactionRecord(txn_rec, workingPointer);
        log_id = txn_rec.GetTransactionId();
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
      case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        GetTupleRecordHeader(*tuple_record, workingPointer);
        log_id = tuple_record->GetTransactionId();
        auto table = LoggingUtil::GetTable(*tuple_record);
        if (recovery_txn_table.find(log_id) == recovery_txn_table.end()) {
          LOG_ERROR("Insert txd id %d not found in recovery txn table",
                    (int)log_id);
          return;
        }

        // Read off the tuple record body from the log
        tuple_record->SetTuple(ReadTupleRecordBody(
            table->GetSchema(), recovery_pool, workingPointer));
        break;
      }
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
      case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
        tuple_record = new TupleRecord(record_type);
        // Check for torn log write
        GetTupleRecordHeader(*tuple_record, workingPointer);

        log_id = tuple_record->GetTransactionId();
        if (recovery_txn_table.find(log_id) == recovery_txn_table.end()) {
          LOG_TRACE("Delete txd id %d not found in recovery txn table",
                    (int)log_id);
          return;
        }
        break;
      }
      default:
        break;
    }
    switch (record_type) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        PL_ASSERT(log_id != INVALID_CID);
        StartTransactionRecovery(log_id);
        break;

      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        PL_ASSERT(log_id != INVALID_CID);
        // do nothing here because we only want to replay when the delimiter is
        // hit
        break;

      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
      case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WBL_TUPLE_DELETE:
      case LOGRECORD_TYPE_WBL_TUPLE_UPDATE:
        recovery_txn_table[tuple_record->GetTransactionId()].push_back(
            tuple_record);
        break;
      case LOGRECORD_TYPE_ITERATION_DELIMITER: {
        // commit all transactions up to this delimeter
        for (auto it = recovery_txn_table.begin();
             it != recovery_txn_table.end();) {
          if (it->first > log_id) {
            break;
          } else {
            CommitTransactionRecovery(it->first);
            recovery_txn_table.erase(it++);
          }
        }
        // if we are syncing wait for flush
        if (wait_for_sync) {
          LogManager::GetInstance().WaitForFlush(log_id);
        }
        break;
      }

      default:
        break;
    }
  }
  // After finishing recovery, set the next oid with maximum oid
  // observed during the recovery
  manager.UpdateCatalogAndTxnManagers(max_oid, 0);

  response->set_sequence_number(curr_seq);
  replication_sequence_number_++;
  LOG_INFO("message len: %lu", request->log().size());
  LOG_INFO("In log record replay service");
}

}  // namespace networking
}  // namespace peloton
