//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down.cpp
//
// Identification: src/codegen/multi_thread/count_down.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread/count_down.h"

namespace peloton {
namespace codegen {

void CountDown::Init(int32_t count) {
  // Call constructor explicitly on memory buffer.
  new (this) CountDown(count);
}

void CountDown::Destroy() {
  // Call destructor explicitly on memory buffer.
  this->~CountDown();
}

void CountDown::Decrease() {
  std::unique_lock<decltype(mutex_)> lock(mutex_);
  int32_t new_count = --count_;
  lock.unlock();

  if (new_count == 0) {
    // If after atomic decrease, the count becomes 0, notify the waiter.
    cv_.notify_one();
  }
}

void CountDown::Wait() {
  std::unique_lock<decltype(mutex_)> lock(mutex_);
  cv_.wait(lock, [&] { return count_ == 0; });
}

}  // namespace codegen
}  // namespace peloton
