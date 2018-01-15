//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// spin_lock.h
//
// Identification: src/include/common/synchronization/spin_lock.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "common/platform.h"
#include "common/macros.h"

//===--------------------------------------------------------------------===//
// Cheap & Easy Spin Lock
//===--------------------------------------------------------------------===//

namespace peloton {
namespace common {
namespace synchronization {

enum class LockState : bool { Unlocked = 0, Locked };

class Spinlock {
 public:
  Spinlock() : spin_lock_state(LockState::Unlocked) {}

  void Lock() {
    while (!TryLock()) {
      _mm_pause();  // helps the cpu to detect busy-wait loop
    }
  }

  bool IsLocked() { return spin_lock_state.load() == LockState::Locked; }

  bool TryLock() {
    // exchange returns the value before locking, thus we need
    // to make sure the lock wasn't already in Locked state before
    return spin_lock_state.exchange(LockState::Locked, std::memory_order_acquire) !=
        LockState::Locked;
  }

  void Unlock() {
    spin_lock_state.store(LockState::Unlocked, std::memory_order_release);
  }

 private:
  /*the exchange method on this atomic is compiled to a lockfree xchgl
   * instruction*/
  std::atomic<LockState> spin_lock_state;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton