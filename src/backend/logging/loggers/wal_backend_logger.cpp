//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.cpp
//
// Identification: src/backend/logging/loggers/wal_backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"

namespace peloton {
namespace logging {

WriteAheadBackendLogger::WriteAheadBackendLogger()
    : log_buffer_(std::unique_ptr<LogBuffer>(nullptr)),
      available_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())),
      persist_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())) {
  logging_type = LOGGING_TYPE_DRAM_NVM;
}

/**
 * @brief log LogRecord
 * @param log record
 */
void WriteAheadBackendLogger::Log(LogRecord *record) {
  // Enqueue the serialized log record into the queue
  record->Serialize(output_buffer);

  this->log_buffer_lock.Lock();
  if (!log_buffer_) {
    LOG_INFO("Acquire the first log buffer in backend logger");
    log_buffer_ = std::move(available_buffer_pool_->Get());
  }
  // update max logged commit id
  if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
    auto new_log_commit_id = record->GetTransactionId();
    assert(new_log_commit_id > highest_logged_commit_message);
    highest_logged_commit_message = new_log_commit_id;
  }
  if (!log_buffer_->WriteRecord(record)) {
    LOG_INFO("Log buffer is full - Attempt to acquire a new one");
    // put back a buffer

    log_buffer_->SetHighestCommittedTransaction(highest_logged_commit_message);
    // we only need to add set the lower bound if it is not superseded by a
    // commit message
    if (logging_cid_lower_bound > highest_logged_commit_message) {
      log_buffer_->SetLoggingCidLowerBound(logging_cid_lower_bound);
    }
    persist_buffer_pool_->Put(std::move(log_buffer_));
    // get a new one
    log_buffer_ = std::move(available_buffer_pool_->Get());
    // write to the new log buffer
    auto success = log_buffer_->WriteRecord(record);
    if (!success) {
      LOG_ERROR("Write record to log buffer failed");
      return;
    }
  }

  this->log_buffer_lock.Unlock();
}

void WriteAheadBackendLogger::PrepareLogBuffers() {
  this->log_buffer_lock.Lock();
  if (log_buffer_ && log_buffer_->GetSize() > 0) {
    // put back a buffer
    LOG_INFO(
        "Move the current log buffer to buffer pool, "
        "highest_logged_commit_message: %d, logging_cid_lower_bound: %d",
        (int)highest_logged_commit_message, (int)logging_cid_lower_bound);
    log_buffer_->SetHighestCommittedTransaction(highest_logged_commit_message);
    // we only need to add set the lower bound if it is not superseded by a
    // commit message
    if (logging_cid_lower_bound > highest_logged_commit_message) {
      log_buffer_->SetLoggingCidLowerBound(logging_cid_lower_bound);
    }
    persist_buffer_pool_->Put(std::move(log_buffer_));
  }
  this->log_buffer_lock.Unlock();

  auto num_log_buffer = persist_buffer_pool_->GetSize();
  // LOG_INFO("Collect %u log buffers from backend logger", num_log_buffer);
  while (num_log_buffer > 0) {
    local_queue.push_back(persist_buffer_pool_->Get());
    num_log_buffer--;
  }
}

void WriteAheadBackendLogger::GrantEmptyBuffer(
    std::unique_ptr<LogBuffer> empty_buffer) {
  available_buffer_pool_->Put(std::move(empty_buffer));
}

LogRecord *WriteAheadBackendLogger::GetTupleRecord(
    LogRecordType log_record_type, txn_id_t txn_id, oid_t table_oid,
    oid_t db_oid, ItemPointer insert_location, ItemPointer delete_location,
    void *data) {
  // Build the log record
  switch (log_record_type) {
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_INSERT;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_DELETE;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_UPDATE;
      break;
    }

    default: {
      assert(false);
      break;
    }
  }

  LogRecord *record =
      new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                      delete_location, data, db_oid);

  return record;
}

}  // namespace logging
}  // namespace peloton
