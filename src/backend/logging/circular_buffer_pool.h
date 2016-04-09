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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Buffer Pool
//===--------------------------------------------------------------------===//

template <unsigned int Capacity>
class CircularBufferPool : public BufferPool {
 public:
  CircularBufferPool(const CircularBufferPool &) = delete;
  CircularBufferPool &operator=(const CircularBufferPool &) = delete;
  CircularBufferPool(CircularBufferPool &&) = delete;
  CircularBufferPool &operator=(CircularBufferPool &&) = delete;

  CircularBufferPool(){};
  ~CircularBufferPool(void){};

  bool Put(std::shared_ptr<LogBuffer>);
  std::shared_ptr<LogBuffer> Get();

 private:
  std::atomic<LogBuffer *> buffer_[Capacity];
  std::atomic<unsigned> head_;
  std::atomic<unsigned> tail_;
};

}  // namespace logging
}  // namespace peloton
