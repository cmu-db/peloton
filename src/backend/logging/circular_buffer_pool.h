/*-------------------------------------------------------------------------
 *
 * circular_buffer_pool.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/circular_buffer_pool.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <atomic>
#include "backend/logging/buffer_pool.h"
#include "backend/common/logger.h"
#include <cstring>

#define BUFFER_POOL_SIZE 32

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Buffer Pool
//===--------------------------------------------------------------------===//

class CircularBufferPool : public BufferPool {
 public:
  CircularBufferPool();
  ~CircularBufferPool();

  bool Put(std::shared_ptr<LogBuffer>);
  std::shared_ptr<LogBuffer> Get();

 private:
  // TODO make BUFFER_POOL_SIZE as class template
  std::shared_ptr<LogBuffer> buffers_[BUFFER_POOL_SIZE];
  std::atomic<unsigned int> head_;
  std::atomic<unsigned int> tail_;
};

}  // namespace logging
}  // namespace peloton
