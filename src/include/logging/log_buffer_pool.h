//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_pool.h
//
// Identification: src/logging/log_buffer_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.h"
#include "common/macros.h"
#include "common/platform.h"

#include "logging/log_buffer.h"

namespace peloton {
namespace logging {
  class LogBufferPool {
    LogBufferPool(const LogBufferPool &) = delete;
    LogBufferPool &operator=(const LogBufferPool &) = delete;
    LogBufferPool(const LogBufferPool &&) = delete;
    LogBufferPool &operator=(const LogBufferPool &&) = delete;

  public:
    LogBufferPool(const size_t thread_id) : 
      head_(0), 
      tail_(buffer_queue_size_),
      thread_id_(thread_id),
      local_buffer_queue_(buffer_queue_size_) {}

    std::unique_ptr<LogBuffer> GetBuffer(const size_t current_eid);

    void PutBuffer(std::unique_ptr<LogBuffer> buf);

    inline size_t GetThreadId() const { return thread_id_; }

    inline size_t GetEmptySlotCount() const {
      return tail_.load() - head_.load();
    }

    inline size_t GetMaxSlotCount() const {
      return buffer_queue_size_;
    }

  private:
    static const size_t buffer_queue_size_ = 16;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
    
    size_t thread_id_;

    std::vector<std::unique_ptr<LogBuffer>> local_buffer_queue_;
};

}
}
