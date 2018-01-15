//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// condition.h
//
// Identification: src/include/common/synchronization/condition.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "common/synchronization/mutex_latch.h"

namespace peloton {
namespace common {
namespace synchronization {

static const long int ONE_S_IN_NS = 1000000000;

class Condition {
 public:
  /**
   * The mutex is the DirtyMutexLatch object that must be
   * locked when using the condition.
   * @param mutex
   */
  explicit Condition(DirtyMutexLatch *mutex) : mutex_(mutex) {
    UNUSED_ATTRIBUTE int status = pthread_cond_init(&cond_, NULL);
    PL_ASSERT(status == 0);
  }

  ~Condition() {
    UNUSED_ATTRIBUTE int status = pthread_cond_destroy(&cond_);
    PL_ASSERT(status == 0);
  }

  /**
   * Wait for the condition to be signalled.
   * This must be called with the DirtyMutexLatch held.
   * This must be called within a loop.
   * See man pthread_cond_wait for details.
   */
  void Wait() {
    UNUSED_ATTRIBUTE int status =
        pthread_cond_wait(&cond_, mutex_->RawDirtyMutexLatch());
    PL_ASSERT(status == 0);
  }

  /**
   * Calls timedwait with a relative, instead of an absolute, timeout.
   * @param relative_time
   * @return
   */
  bool TimedwaitRelative(const struct timespec &relative_time) {
    PL_ASSERT(0 <= relative_time.tv_nsec &&
              relative_time.tv_nsec < ONE_S_IN_NS);

    struct timespec absolute;
    // clock_gettime would be more convenient, but that needs librt
    // int status = clock_gettime(CLOCK_REALTIME, &absolute);
    struct timeval tv;
    UNUSED_ATTRIBUTE int status = gettimeofday(&tv, NULL);
    PL_ASSERT(status == 0);
    absolute.tv_sec = tv.tv_sec + relative_time.tv_sec;
    absolute.tv_nsec = tv.tv_usec * 1000 + relative_time.tv_nsec;
    if (absolute.tv_nsec >= ONE_S_IN_NS) {
      absolute.tv_nsec -= ONE_S_IN_NS;
      absolute.tv_sec += 1;
    }
    PL_ASSERT(0 <= absolute.tv_nsec && absolute.tv_nsec < ONE_S_IN_NS);

    return Timedwait(absolute);
  }

  /**
   * Returns true if the lock is acquired, false otherwise.
   * abstime is the *absolute* time.
   * @param absolute_time
   * @return
   */
  bool Timedwait(const struct timespec &absolute_time) {
    PL_ASSERT(0 <= absolute_time.tv_nsec &&
              absolute_time.tv_nsec < ONE_S_IN_NS);

    UNUSED_ATTRIBUTE int status = pthread_cond_timedwait(
        &cond_, mutex_->RawDirtyMutexLatch(), &absolute_time);
    if (status == ETIMEDOUT) {
      return false;
    }
    PL_ASSERT(status == 0);
    return true;
  }

  void Signal() {
    UNUSED_ATTRIBUTE int status = pthread_cond_signal(&cond_);
    PL_ASSERT(status == 0);
  }

  /**
   * Wake all threads that are waiting on this condition.
   */
  void Broadcast() {
    UNUSED_ATTRIBUTE int status = pthread_cond_broadcast(&cond_);
    PL_ASSERT(status == 0);
  }

 private:
  // Not copyable
  Condition(const Condition &other);
  Condition &operator=(const Condition &other);

  pthread_cond_t cond_;
  DirtyMutexLatch *mutex_;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton
