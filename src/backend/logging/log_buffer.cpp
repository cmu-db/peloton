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
    LOG_INFO("highest_logged_commit_id: %lu, record->GetTransactionId(): %lu",
             highest_commit_id_, new_log_commit_id);
    assert(new_log_commit_id > highest_commit_id_);
    highest_commit_id_ = new_log_commit_id;
  }
  return true;
}

cid_t LogBuffer::GetHighestCommitId() { return highest_commit_id_; }

void LogBuffer::SetHighestCommitId(cid_t highest_commit_id) {
  // LOG_INFO("compare highest_commit_id_ %lu <= highest_commit_id %lu",
  //		highest_commit_id_, highest_commit_id);
  LOG_INFO("highest_commit self : %d, new : %d", (int)highest_commit_id_, (int)highest_commit_id);
  assert(highest_commit_id_ <= highest_commit_id);
  highest_commit_id_ = highest_commit_id;
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
