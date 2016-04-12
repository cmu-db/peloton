/*-------------------------------------------------------------------------
 *
 * buffer_pool.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/buffer_pool.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/log_buffer.h"
#include <memory>

#define BUFFER_POOL_SIZE 8

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Buffer Pool
//===--------------------------------------------------------------------===//
// TODO make BUFFER_POOL_SIZE as class template
class BufferPool {
 public:
  virtual ~BufferPool() {}

  // put a buffer to the buffer pool
  virtual bool Put(std::unique_ptr<LogBuffer>) = 0;

  // get a buffer from the buffer pool
  virtual std::unique_ptr<LogBuffer> Get() = 0;

  virtual unsigned int GetSize() = 0;
};

}  // namespace logging
}  // namespace peloton
