//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_pool.cpp
//
// Identification: src/logging/log_buffer_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "logging/log_buffer_pool.h"
#include "common/logger.h"

namespace peloton {
namespace logging {

  // Acquire a log buffer from the buffer pool.
  // This function will be blocked until there is an available buffer.
  // Note that only the corresponding worker thread can call this function.
   std::unique_ptr<LogBuffer> LogBufferPool::GetBuffer() {
      std::unique_ptr<LogBuffer>buf(new LogBuffer());
    return std::move(buf);
  }

  // This function is called only by the corresponding logger.
  void LogBufferPool::PutBuffer(std::unique_ptr<LogBuffer> buf) {
    PL_ASSERT(buf.get() != nullptr);
    PL_ASSERT(buf->GetThreadId() == thread_id_);

    size_t tail_idx = tail_ % buffer_queue_size_;
    
    // The buffer pool must not be full
    PL_ASSERT(tail_idx != head_ % buffer_queue_size_);
    // The tail pos must be null
    PL_ASSERT(local_buffer_queue_[tail_idx].get() == nullptr);
    // The returned buffer must be empty
    PL_ASSERT(buf->Empty() == true);
    local_buffer_queue_[tail_idx].reset(buf.release());
    
    tail_.fetch_add(1, std::memory_order_relaxed);
  }

}
}
