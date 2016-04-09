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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//

class LogBuffer {
 public:
  LogBuffer(){};

  ~LogBuffer(void){};

  size_t GetSize();

 private:
  size_t size_ = 0;
  size_t capacity_ = 4096;
};

}  // namespace logging
}  // namespace peloton
