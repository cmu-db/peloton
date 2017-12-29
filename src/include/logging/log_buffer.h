//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer.h
//
// Identification: src/backend/logging/log_buffer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "common/internal_types.h"

namespace peloton {
namespace logging {

class LogBuffer {
  LogBuffer(const LogBuffer &) = delete;
  LogBuffer &operator=(const LogBuffer &) = delete;
  LogBuffer(LogBuffer &&) = delete;
  LogBuffer &operator=(LogBuffer &&) = delete;

  friend class LogBufferPool;
private:
  // constant
  const static size_t log_buffer_capacity_ = 1024 * 1024 * 32; // 32 MB

public:
  LogBuffer(const size_t thread_id, const size_t eid) : 
      thread_id_(thread_id), eid_(eid), size_(0){
    data_ = new char[log_buffer_capacity_];
    PL_MEMSET(data_, 0, log_buffer_capacity_);
  }
  ~LogBuffer() {
    delete[] data_;
    data_ = nullptr;
  }

  inline void Reset() { size_ = 0; eid_ = INVALID_EID; }

  inline char *GetData() { return data_; }

  inline size_t GetSize() { return size_; }

  inline size_t GetEpochId() { return eid_; }

  inline size_t GetThreadId() { return thread_id_; }

  inline bool Empty() { return size_ == 0; }

  bool WriteData(const char *data, size_t len);

private:
  size_t thread_id_;
  size_t eid_;
  size_t size_;
  char* data_;
};

}
}
