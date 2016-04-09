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
#define BUFFER_POOL_MASK (BUFFER_POOL_SIZE - 1)
namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Buffer Pool
//===--------------------------------------------------------------------===//

class CircularBufferPool : public BufferPool {
 public:
  CircularBufferPool();
  ~CircularBufferPool();

  bool Put(std::unique_ptr<LogBuffer>);
  std::unique_ptr<LogBuffer> Get();

 private:
  // TODO make BUFFER_POOL_SIZE as class template
  std::unique_ptr<LogBuffer> buffers_[BUFFER_POOL_SIZE];
  std::atomic<unsigned int> head_;
  std::atomic<unsigned int> tail_;
};

}  // namespace logging
}  // namespace peloton
