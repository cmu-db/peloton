//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down_latch.h
//
// Identification: src/include/common/synchronization/count_down_latch.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <mutex>
#include <chrono>

#include "common/macros.h"

namespace peloton {
namespace common {
namespace synchronization {

/**
 *
 */
class CountDownLatch {
 public:
  /// Constructor
  explicit CountDownLatch(uint64_t count);

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CountDownLatch);

  /// Wait for the latch to be triggered, up to a maximum of 'nanos' nanoseconds
  bool Await(uint64_t nanos);

  /// Count down the latch by one
  void CountDown();

  /// Get the current count of the latch
  uint32_t GetCount();

 private:
  uint64_t count_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline CountDownLatch::CountDownLatch(uint64_t count) : count_(count) {}

inline bool CountDownLatch::Await(uint64_t nanos) {
  std::unique_lock<std::mutex> lock(mutex_);

  // Check count to see if the latch is complete
  if (count_ == 0) {
    return true;
  }

  // Latch is not complete, either wait for completion or a specified time
  if (nanos == 0) {
    cv_.wait(lock, [this]() noexcept { return count_ == 0; });
    return true;
  } else {
    return cv_.wait_for(lock, std::chrono::nanoseconds(nanos),
                        [this]() noexcept { return count_ == 0; });
  }
}

inline void CountDownLatch::CountDown() {
  std::lock_guard<std::mutex> lock(mutex_);

  // If the count is already zero, nothing to do, no one to notify
  if (count_ == 0) {
    return;
  }

  // Decrement count
  count_--;

  // Are we the last?
  if (count_ == 0) {
    cv_.notify_all();
  }
}

inline uint32_t CountDownLatch::GetCount() {
  std::lock_guard<std::mutex> lock(mutex_);
  return count_;
}

}  // namespace synchronization
}  // namespace common
}  // namespace peloton
