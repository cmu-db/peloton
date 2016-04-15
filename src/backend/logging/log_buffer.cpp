/*-------------------------------------------------------------------------
 *
 * log_buffer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/log_buffer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/log_buffer.h"
#include "backend/common/logger.h"

#include <cstring>
#include <cassert>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//
LogBuffer::LogBuffer(BackendLogger *backend_logger)
    : backend_logger_(backend_logger) {
  memset(data_, 0, LOG_BUFFER_CAPACITY * sizeof(char));
}

bool LogBuffer::WriteRecord(LogRecord *record) {
  bool success = WriteData(record->GetMessage(), record->GetMessageLength());
  if (!success) {
    return success;
  }
  // update max commit id
  if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
    auto new_log_commit_id = record->GetTransactionId();
    LOG_INFO(
        "highest_committed_transaction_: %lu, logging_cid_lower_bound_ %lu, "
        "record->GetTransactionId(): %lu",
        highest_committed_transaction_, logging_cid_lower_bound_,
        new_log_commit_id);
    assert(new_log_commit_id > highest_committed_transaction_);
    assert(new_log_commit_id > logging_cid_lower_bound_);
    highest_committed_transaction_ = new_log_commit_id;
    logging_cid_lower_bound_ = INVALID_CID;
  }
  return true;
}

cid_t LogBuffer::GetHighestCommittedTransaction() {
  return highest_committed_transaction_;
}

void LogBuffer::SetHighestCommittedTransaction(cid_t highest_commit_id) {
  assert(highest_committed_transaction_ <= highest_commit_id);
  highest_committed_transaction_ = highest_commit_id;
}

cid_t LogBuffer::GetLoggingCidLowerBound() { return logging_cid_lower_bound_; }

void LogBuffer::SetLoggingCidLowerBound(cid_t cid_lower_bound) {
  assert(logging_cid_lower_bound_ <= cid_lower_bound);
  logging_cid_lower_bound_ = cid_lower_bound;
}

char *LogBuffer::GetData() { return data_; }

void LogBuffer::ResetData() {
  size_ = 0;
  memset(data_, 0, LOG_BUFFER_CAPACITY * sizeof(char));
}

// Internal Methods
bool LogBuffer::WriteData(char *data, size_t len) {
  // Not enough space
  if (len + size_ > capacity_) {
    return false;
  }
  assert(data);
  assert(len);
  std::memcpy(data_ + size_, data, len);
  size_ += len;
  return true;
}

size_t LogBuffer::GetSize() { return size_; }

void LogBuffer::SetSize(size_t size) {
  assert(size < capacity_);
  size_ = size;
}

BackendLogger *LogBuffer::GetBackendLogger() { return backend_logger_; }

}  // namespace logging
}  // namespace peloton
