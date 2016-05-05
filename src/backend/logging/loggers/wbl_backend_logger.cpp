//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_backend_logger.cpp
//
// Identification: src/backend/logging/loggers/wbl_backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"

namespace peloton {
namespace logging {

void WriteBehindBackendLogger::Log(LogRecord *record) {
  log_buffer_lock.Lock();
  switch (record->GetType()) {
    case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      highest_logged_commit_message = record->GetTransactionId();
    // fallthrough
    case LOGRECORD_TYPE_TRANSACTION_ABORT:
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_DONE:
    case LOGRECORD_TYPE_TRANSACTION_END: {
      // TODO this is a hack we should fix this once correctness is guaranteed.
      TransactionRecord *original = (TransactionRecord *)record;
      TransactionRecord *txn_rec_cpy = new TransactionRecord(
          original->GetType(), original->GetTransactionId());
      wbl_record_queue.push_back(std::unique_ptr<LogRecord>(txn_rec_cpy));
      break;
    }
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE:
    case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
      // TODO this is a hack we should fix this once correctness is guaranteed.
      TupleRecord *original = (TupleRecord *)record;
      LogRecord *tuple_rec_cpy =
          GetTupleRecord(original->GetType(), original->GetTransactionId(),
                         original->GetTableId(), original->GetDatabaseOid(),
                         original->GetInsertLocation(),
                         original->GetDeleteLocation(), nullptr);
      wbl_record_queue.push_back(std::unique_ptr<LogRecord>(tuple_rec_cpy));
      break;
    }
    default:
      LOG_INFO("Invalid log record type");
      break;
  }

  log_buffer_lock.Unlock();
}

void WriteBehindBackendLogger::CollectRecordsAndClear(
    std::vector<std::unique_ptr<LogRecord>> &frontend_queue) {
  log_buffer_lock.Lock();
  frontend_queue.swap(wbl_record_queue);
  log_buffer_lock.Unlock();
}

LogRecord *WriteBehindBackendLogger::GetTupleRecord(
    LogRecordType log_record_type, txn_id_t txn_id, oid_t table_oid,
    oid_t db_oid, ItemPointer insert_location, ItemPointer delete_location,
    __attribute__((unused)) const void *data) {
  // Figure the log record type
  switch (log_record_type) {
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_INSERT;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_DELETE;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_UPDATE;
      break;
    }

    default: {
      assert(false);
      break;
    }
  }

  // Don't make use of "data" in case of peloton log records
  // Build the tuple log record
  LogRecord *tuple_record =
      new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                      delete_location, nullptr, db_oid);

  return tuple_record;
}

}  // namespace logging
}  // namespace peloton
