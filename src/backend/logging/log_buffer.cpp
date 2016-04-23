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
#include "backend/logging/log_manager.h"

#include <cstring>
#include <cassert>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//
LogBuffer::LogBuffer(BackendLogger *backend_logger)
    : backend_logger_(backend_logger) {
  capacity_ = LogManager::GetInstance().GetLogBufferCapacity();
  elastic_data_.reset(new char[capacity_]());
  memset(elastic_data_.get(), 0, capacity_ * sizeof(char));
}

bool LogBuffer::WriteRecord(LogRecord *record) {
  bool success = WriteData(record->GetMessage(), record->GetMessageLength());
  return success;
}

char *LogBuffer::GetData() { return elastic_data_.get(); }

void LogBuffer::ResetData() {
  size_ = 0;
  memset(elastic_data_.get(), 0, capacity_ * sizeof(char));
}

// Internal Methods
bool LogBuffer::WriteData(char *data, size_t len) {
  // Not enough space
  while (len + size_ > capacity_) {
    if (size_ == 0) {
      // double log buffer capacity for empty buffer
      capacity_ *= 2;
      elastic_data_.reset(new char[capacity_]);
    } else {
      return false;
    }
  }
  assert(data);
  assert(len);
  std::memcpy(elastic_data_.get() + size_, data, len);
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
