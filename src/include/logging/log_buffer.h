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
  const static size_t log_buffer_capacity_ = 1024 * 512;  // 512 KB

 public:
  LogBuffer() : size_(0) {
    data_ = new char[log_buffer_capacity_];
    PL_MEMSET(data_, 0, log_buffer_capacity_);
  }
  ~LogBuffer() {
    delete[] data_;
    data_ = nullptr;
  }

  inline void Reset() { size_ = 0; }  // eid_ = INVALID_EID; }

  inline char *GetData() { return data_; }

  inline size_t GetSize() { return size_; }

  inline bool Empty() { return size_ == 0; }

  bool WriteData(const char *data, size_t len);

 private:
  size_t size_;
  char *data_;
};
}
}
