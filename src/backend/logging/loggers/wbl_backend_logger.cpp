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
  // if we are committing, sync all data before taking the lock
  if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
    SyncDataForCommit();
  }
  log_buffer_lock.Lock();
  cid_t cur_log_id = record->GetTransactionId();
  switch (record->GetType()) {
    case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      highest_logged_commit_message = record->GetTransactionId();
    // fallthrough
    case LOGRECORD_TYPE_TRANSACTION_ABORT:
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_DONE:
    case LOGRECORD_TYPE_TRANSACTION_END: {
      if (logging_cid_lower_bound < record->GetTransactionId() - 1) {
        logging_cid_lower_bound = record->GetTransactionId() - 1;
      }
      break;
    }
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
      tile_groups_to_sync_.insert(
          ((TupleRecord *)record)->GetDeleteLocation().block);
      break;
    }
    case LOGRECORD_TYPE_WBL_TUPLE_INSERT: {
      tile_groups_to_sync_.insert(
          ((TupleRecord *)record)->GetInsertLocation().block);
      break;
    }
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
      tile_groups_to_sync_.insert(
          ((TupleRecord *)record)->GetDeleteLocation().block);
      tile_groups_to_sync_.insert(
          ((TupleRecord *)record)->GetInsertLocation().block);
      break;
    }
    default:
      LOG_INFO("Invalid log record type");
      break;
  }

  if (replicating_) {
    // here we should be getting LogRecords with data from the log manager
    if (!log_buffer_) {
      LOG_TRACE("Acquire the first log buffer in backend logger");
      this->log_buffer_lock.Unlock();
      std::unique_ptr<LogBuffer> new_buff =
          std::move(available_buffer_pool_->Get());
      this->log_buffer_lock.Lock();
      log_buffer_ = std::move(new_buff);
    }
    // set if this is the max log_id seen so far
    if (cur_log_id > max_log_id_buffer) {
      log_buffer_->SetMaxLogId(cur_log_id);
      max_log_id_buffer = cur_log_id;
    }

    if (!log_buffer_->WriteRecord(record)) {
      LOG_TRACE("Log buffer is full - Attempt to acquire a new one");
      // put back a buffer
      max_log_id_buffer = 0;  // reset
      persist_buffer_pool_->Put(std::move(log_buffer_));
      this->log_buffer_lock.Unlock();

      // get a new one
      std::unique_ptr<LogBuffer> new_buff =
          std::move(available_buffer_pool_->Get());
      this->log_buffer_lock.Lock();
      log_buffer_ = std::move(new_buff);

      // write to the new log buffer
      auto success = log_buffer_->WriteRecord(record);
      if (!success) {
        LOG_ERROR("Write record to log buffer failed");
        this->log_buffer_lock.Unlock();
        return;
      }
    }
  }

  log_buffer_lock.Unlock();
}

void WriteBehindBackendLogger::SyncDataForCommit() {
  auto &manager = catalog::Manager::GetInstance();

  // Sync the tiles in the modified tile groups and their headers
  for (oid_t tile_group_id : tile_groups_to_sync_) {
    auto tile_group = manager.GetTileGroup(tile_group_id);
    tile_group->Sync();
    tile_group->GetHeader()->Sync();
  }

  // Clear the list of tile groups
  tile_groups_to_sync_.clear();
}

LogRecord *WriteBehindBackendLogger::GetTupleRecord(
    LogRecordType log_record_type, txn_id_t txn_id, oid_t table_oid,
    oid_t db_oid, ItemPointer insert_location, ItemPointer delete_location,
    UNUSED_ATTRIBUTE const void *data) {
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
      PL_ASSERT(false);
      break;
    }
  }
  LogRecord *tuple_record;
  if (replicating_) {
    tuple_record =
        new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                        delete_location, data, db_oid);
  } else {
    // Don't make use of "data" in case of peloton log records
    // Build the tuple log record
    tuple_record =
        new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                        delete_location, nullptr, db_oid);
  }

  return tuple_record;
}

}  // namespace logging
}  // namespace peloton
