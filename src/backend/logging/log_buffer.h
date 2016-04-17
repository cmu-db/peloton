/*-------------------------------------------------------------------------
 *
 * log_buffer.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/log_buffer.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstddef>
#include <cassert>
#include <memory>
#include "backend/logging/log_record.h"

#define LOG_BUFFER_CAPACITY 32768

namespace peloton {
namespace logging {

class BackendLogger;

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//

// TODO make capacity_ template parameter
class LogBuffer {
 public:
  LogBuffer(BackendLogger *);

  ~LogBuffer(void){};

  char *GetData();

  bool WriteRecord(LogRecord *);

  void ResetData();

  size_t GetSize();

  void SetSize(size_t size);

  void SetMaxLogId(cid_t new_max) { max_log_id = new_max; }

  cid_t GetMaxLogId() { return max_log_id; }

  BackendLogger *GetBackendLogger();

 private:
  // write data to the log buffer, return false if not enough space
  bool WriteData(char *data, size_t len);

  // the size of buffer used already
  size_t size_ = 0;

  // the total capacity of the buffer
  size_t capacity_ = LOG_BUFFER_CAPACITY;

  char data_[LOG_BUFFER_CAPACITY];

  BackendLogger *backend_logger_;

  cid_t max_log_id = 0;
};

}  // namespace logging
}  // namespace peloton
