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

  cid_t GetHighestCommittedTransaction();

  void SetHighestCommittedTransaction(cid_t highest_commit_id);

  cid_t GetLoggingCidLowerBound();

  void SetLoggingCidLowerBound(cid_t cid_lower_bound);

  size_t GetSize();

  void SetSize(size_t size);

  BackendLogger *GetBackendLogger();

 private:
  // write data to the log buffer, return false if not enough space
  bool WriteData(char *data, size_t len);

  // the size of buffer used already
  size_t size_ = 0;

  // the total capacity of the buffer
  size_t capacity_ = LOG_BUFFER_CAPACITY;

  char data_[LOG_BUFFER_CAPACITY];

  // the highest commit id
  cid_t highest_committed_transaction_ = 0;

  cid_t logging_cid_lower_bound_ = 0;

  BackendLogger *backend_logger_;
};

}  // namespace logging
}  // namespace peloton
