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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Buffer Pool
//===--------------------------------------------------------------------===//
class BufferPool {
 public:
  BufferPool(const BufferPool &) = delete;
  BufferPool &operator=(const BufferPool &) = delete;
  BufferPool(BufferPool &&) = delete;
  BufferPool &operator=(BufferPool &&) = delete;

  virtual ~BufferPool(void);

  virtual bool Put(std::shared_ptr<LogBuffer>);
  virtual std::shared_ptr<LogBuffer> Get();
};

}  // namespace logging
}  // namespace peloton
