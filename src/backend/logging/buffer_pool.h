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
  virtual ~BufferPool() {}

  virtual bool Put(std::shared_ptr<LogBuffer>) = 0;
  virtual std::shared_ptr<LogBuffer> Get() = 0;
};

}  // namespace logging
}  // namespace peloton
