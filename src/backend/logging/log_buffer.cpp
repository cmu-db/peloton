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
#include "backend/logging/log_manager.h"
#include "backend/common/logger.h"
#include "backend/common/macros.h"

#include <cstring>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//
LogBuffer::LogBuffer(BackendLogger *backend_logger)
    : backend_logger_(backend_logger) {
  capacity_ = LogManager::GetInstance().GetLogBufferCapacity();
  elastic_data_.reset(new char[capacity_]);
}

bool LogBuffer::WriteRecord(LogRecord *record) {
  bool success = WriteData(record->GetMessage(), record->GetMessageLength());
  return success;
}

void LogBuffer::ResetData() {
  size_ = 0;
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
  ALWAYS_ASSERT(data);
  ALWAYS_ASSERT(len);
  PL_MEMCPY(elastic_data_.get() + size_, data, len);
  size_ += len;
  return true;
}

}  // namespace logging
}  // namespace peloton
