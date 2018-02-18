//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer.cpp
//
// Identification: src/logging/log_buffer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/macros.h"
#include "logging/log_buffer.h"

namespace peloton {
namespace logging {

bool LogBuffer::WriteData(const char *data, size_t len) {
  if (unlikely_branch(size_ + len > log_buffer_capacity_)) {
    return false;
  } else {
    PELOTON_ASSERT(data);
    PELOTON_ASSERT(len);
    PELOTON_MEMCPY(data_ + size_, data, len);
    size_ += len;
    return true;
  }
}

}
}
