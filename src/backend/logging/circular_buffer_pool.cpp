/*-------------------------------------------------------------------------
 *
 * circular_buffer_pool.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/circular_buffer_pool.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <xmmintrin.h>

#include "backend/logging/circular_buffer_pool.h"
#include "backend/common/logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Circular Buffer Pool
//===--------------------------------------------------------------------===//
CircularBufferPool::CircularBufferPool()
    : head_(ATOMIC_VAR_INIT(0)), tail_(ATOMIC_VAR_INIT(0)) {
  memset(buffers_, 0, BUFFER_POOL_SIZE * sizeof(std::unique_ptr<LogBuffer>));
}

CircularBufferPool::~CircularBufferPool() {
  for (unsigned int i = GET_BUFFER_POOL_INDEX(tail_);
       i < GET_BUFFER_POOL_INDEX(head_); i++) {
    buffers_[i].release();
  }
}

bool CircularBufferPool::Put(std::unique_ptr<LogBuffer> buffer) {
  // assume enough place for put a buffer
  unsigned int current_idx = GET_BUFFER_POOL_INDEX(head_.fetch_add(1));
  LOG_TRACE("CircularBufferPool::Put - current_idx: %u", current_idx);
  buffers_[current_idx] = std::move(buffer);
  return true;
}

std::unique_ptr<LogBuffer> CircularBufferPool::Get() {
  unsigned int current_idx = GET_BUFFER_POOL_INDEX(tail_.fetch_add(1));
  while (true) {
    if (buffers_[current_idx]) {
      break;
    } else {
      // pause for a minimum amount of time
      _mm_pause();
    }
  }
  LOG_TRACE("CircularBufferPool::Get - current_idx: %u", current_idx);
  std::unique_ptr<LogBuffer> buff = std::move(buffers_[current_idx]);
  memset(buffers_ + current_idx, 0, sizeof(std::unique_ptr<LogBuffer>));
  return buff;
}

unsigned int CircularBufferPool::GetSize() {
  auto head = head_.load();
  auto tail = tail_.load();

  // get the offset of head and tail
  int size = GET_BUFFER_POOL_INDEX(head) - GET_BUFFER_POOL_INDEX(tail);
  if (size == 0 && head - tail > 0) {
    return BUFFER_POOL_SIZE;
  }
  return (size + BUFFER_POOL_SIZE) % BUFFER_POOL_SIZE;
}

}  // namespace logging
}  // namespace peloton
