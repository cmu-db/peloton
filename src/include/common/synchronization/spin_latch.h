//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// spin_latch.h
//
// Identification: src/include/common/synchronization/spin_latch.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "common/platform.h"
#include "common/macros.h"

//===--------------------------------------------------------------------===//
// Cheap & Easy Spin Latch
//===--------------------------------------------------------------------===//

namespace peloton {
namespace common {
namespace synchronization {

enum class LatchState : bool { Unlocked = 0, Locked };

class SpinLatch {
 public:
  SpinLatch() : state_(LatchState::Unlocked) {}

  void Lock() {
    while (!TryLock()) {
      _mm_pause();  // helps the cpu to detect busy-wait loop
    }
  }

  bool IsLocked() { return state_.load() == LatchState::Locked; }

  bool TryLock() {
    // exchange returns the value before locking, thus we need
    // to make sure the lock wasn't already in Locked state before
    return state_.exchange(LatchState::Locked, std::memory_order_acquire) !=
           LatchState::Locked;
  }

  void Unlock() {
    state_.store(LatchState::Unlocked, std::memory_order_release);
  }

 private:
  /*the exchange method on this atomic is compiled to a lockfree xchgl
   * instruction*/
  std::atomic<LatchState> state_;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton