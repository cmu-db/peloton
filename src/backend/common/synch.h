//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// synch.h
//
// Identification: src/backend/common/synch.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <atomic>

#include <pthread.h>
#include <immintrin.h>

//===--------------------------------------------------------------------===//
// Synchronization utilities
//===--------------------------------------------------------------------===//

namespace peloton {

template <typename T>
inline bool atomic_cas(T *object, T old_value, T new_value) {
  return __sync_bool_compare_and_swap(object, old_value, new_value);
}

//===--------------------------------------------------------------------===//
// Reader/Writer lock
//===--------------------------------------------------------------------===//

// Wrapper around pthread_rwlock_t

struct RWLock {
 public:
  RWLock(RWLock const &) = delete;
  RWLock &operator=(RWLock const &) = delete;

  RWLock() { pthread_rwlock_init(&rw_lock, nullptr); }

  ~RWLock() { pthread_rwlock_destroy(&rw_lock); }

  void ReadLock() const { pthread_rwlock_rdlock(&rw_lock); }

  void WriteLock() const { pthread_rwlock_wrlock(&rw_lock); }

  void Unlock() const { pthread_rwlock_unlock(&rw_lock); }

 private:
  // can only be moved, not copied
  mutable pthread_rwlock_t rw_lock;
};

struct RecursiveLock {
 public:
  RecursiveLock(RecursiveLock const &) = delete;
  RecursiveLock &operator=(RecursiveLock const &) = delete;

  RecursiveLock() {
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&recursive_mutex, &attr);
  }

  ~RecursiveLock() { pthread_mutex_destroy(&recursive_mutex); }

  void Lock() const { pthread_mutex_lock(&recursive_mutex); }

  void Unlock() const { pthread_mutex_unlock(&recursive_mutex); }

 private:
  // can only be moved, not copied
  pthread_mutexattr_t attr;
  mutable pthread_mutex_t recursive_mutex;
};

struct PelotonReadLock {
  const RWLock &shared_lock;

  PelotonReadLock(const RWLock &mtx) : shared_lock(mtx) { mtx.ReadLock(); }

  ~PelotonReadLock() { shared_lock.Unlock(); }
};

struct PelotonWriteLock {
  const RWLock &exclusive_lock;

  PelotonWriteLock(const RWLock &mtx) : exclusive_lock(mtx) { mtx.WriteLock(); }

  ~PelotonWriteLock() { exclusive_lock.Unlock(); }
};

//===--------------------------------------------------------------------===//
// Spinlock
//===--------------------------------------------------------------------===//

class Spinlock {
 public:
  Spinlock() : spin_lock_state(Unlocked) {}

  inline void Lock() {
    while (!TryLock()) {
      _mm_pause();  // helps the cpu to detect busy-wait loop
    }
  }

  bool IsLocked() { return spin_lock_state.load() == Locked; }

  inline bool TryLock() {
    // exchange returns the value before locking, thus we need
    // to make sure the lock wasn't already in Locked state before
    return spin_lock_state.exchange(Locked, std::memory_order_acquire) !=
           Locked;
  }

  inline void Unlock() {
    spin_lock_state.store(Unlocked, std::memory_order_release);
  }

 private:
  typedef enum { Locked, Unlocked } LockState;

  /*the exchange method on this atomic is compiled to a lockfree xchgl
   * instruction*/
  std::atomic<LockState> spin_lock_state;
};

}  // End peloton namespace
