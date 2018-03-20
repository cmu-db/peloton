//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mutex.h
//
// Identification: src/include/common/synchronization/mutex.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <errno.h>
#include <pthread.h>

#include "common/macros.h"

namespace peloton {
namespace common {
namespace synchronization {

/**
 * Wrapper around pthread_mutex_t
 *
 * This class is called 'dirty' because you should not be using
 * this <b>unless</b> you have a good reason (i.e., you are in the
 * networking layer). You probably should be using a SpinLatch instead.
 */
class DirtyMutexLatch {
 public:
  DirtyMutexLatch() {
    UNUSED_ATTRIBUTE int status = pthread_mutex_init(&mutex_, NULL);
    PL_ASSERT(status == 0);
  }

  ~DirtyMutexLatch() {
    UNUSED_ATTRIBUTE int status = pthread_mutex_destroy(&mutex_);
    PL_ASSERT(status == 0);
  }

  void Lock() {
    UNUSED_ATTRIBUTE int status = pthread_mutex_lock(&mutex_);
    PL_ASSERT(status == 0);
  }

  // Returns true if the lock is acquired.
  bool TryLock() {
    UNUSED_ATTRIBUTE int status = pthread_mutex_trylock(&mutex_);
    if (status == 0) return true;
    PL_ASSERT(status == EBUSY);
    return false;
  }

  void UnLock() {
    UNUSED_ATTRIBUTE int status = pthread_mutex_unlock(&mutex_);
    PL_ASSERT(status == 0);
  }

  pthread_mutex_t *RawDirtyMutexLatch() { return &mutex_; }

 private:
  // Not copyable.
  DirtyMutexLatch(const DirtyMutexLatch &other);
  DirtyMutexLatch &operator=(const DirtyMutexLatch &other);

  pthread_mutex_t mutex_;
};

/**
 * Automatic Dirty Mutex
 * Locks mutex in the constructor, unlocks it in the destructor.
 */
class AutomaticDirtyMutexLatch {
 public:
  explicit AutomaticDirtyMutexLatch(DirtyMutexLatch *mutex) : mutex_(mutex) {
    mutex_->Lock();
  }

  ~AutomaticDirtyMutexLatch() { mutex_->UnLock(); }

 private:
  // Not copyable.
  AutomaticDirtyMutexLatch(const AutomaticDirtyMutexLatch &other);
  AutomaticDirtyMutexLatch &operator=(const AutomaticDirtyMutexLatch &other);

  DirtyMutexLatch *mutex_;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton
