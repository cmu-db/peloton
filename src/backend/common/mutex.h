//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mutex.h
//
// Identification: src/backend/common/mutex.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "backend/common/assert.h"

namespace peloton {

// Wrapper around pthread_mutex_t
class Mutex {
 public:
  Mutex() {
    int status = pthread_mutex_init(&mutex_, NULL);
    ASSERT(status == 0);
  }

  ~Mutex() {
    int status = pthread_mutex_destroy(&mutex_);
    ASSERT(status == 0);
  }

  void Lock() {
    int status = pthread_mutex_lock(&mutex_);
    ASSERT(status == 0);
  }

  // Returns true if the lock is acquired.
  bool TryLock() {
    int status = pthread_mutex_trylock(&mutex_);
    if (status == 0) return true;
    ASSERT(status == EBUSY);
    return false;
  }

  void UnLock() {
    int status = pthread_mutex_unlock(&mutex_);
    ASSERT(status == 0);
  }

  pthread_mutex_t* RawMutex() { return &mutex_; }

 private:
  // Not copyable.
  Mutex(const Mutex& other);
  Mutex& operator=(const Mutex& other);

  pthread_mutex_t mutex_;
};

// Locks mutex in the constructor, unlocks it in the destructor.
class MutexLock {
 public:
  explicit MutexLock(Mutex* mutex) : mutex_(mutex) { mutex_->Lock(); }

  ~MutexLock() { mutex_->UnLock(); }

 private:
  // Not copyable.
  MutexLock(const MutexLock& other);
  MutexLock& operator=(const MutexLock& other);

  Mutex* mutex_;
};

static const long int ONE_S_IN_NS = 1000000000;

class Condition {
 public:
  // mutex is the Mutex that must be locked when using the condition.
  explicit Condition(Mutex* mutex) : mutex_(mutex) {
    int status = pthread_cond_init(&cond_, NULL);
    ASSERT(status == 0);
  }

  ~Condition() {
    int status = pthread_cond_destroy(&cond_);
    ASSERT(status == 0);
  }

  // Wait for the condition to be signalled. This must be called with the Mutex
  // held.
  // This must be called within a loop. See man pthread_cond_wait for details.
  void Wait() {
    int status = pthread_cond_wait(&cond_, mutex_->RawMutex());
    ASSERT(status == 0);
  }

  // Calls timedwait with a relative, instead of an absolute, timeout.
  bool TimedwaitRelative(const struct timespec& relative_time) {
    ASSERT(0 <= relative_time.tv_nsec && relative_time.tv_nsec < ONE_S_IN_NS);

    struct timespec absolute;
    // clock_gettime would be more convenient, but that needs librt
    // int status = clock_gettime(CLOCK_REALTIME, &absolute);
    struct timeval tv;
    int status = gettimeofday(&tv, NULL);
    ASSERT(status == 0);
    absolute.tv_sec = tv.tv_sec + relative_time.tv_sec;
    absolute.tv_nsec = tv.tv_usec * 1000 + relative_time.tv_nsec;
    if (absolute.tv_nsec >= ONE_S_IN_NS) {
      absolute.tv_nsec -= ONE_S_IN_NS;
      absolute.tv_sec += 1;
    }
    ASSERT(0 <= absolute.tv_nsec && absolute.tv_nsec < ONE_S_IN_NS);

    return Timedwait(absolute);
  }

  // Returns true if the lock is acquired, false otherwise. abstime is the
  // *absolute* time.
  bool Timedwait(const struct timespec& absolute_time) {
    ASSERT(0 <= absolute_time.tv_nsec && absolute_time.tv_nsec < ONE_S_IN_NS);

    int status =
        pthread_cond_timedwait(&cond_, mutex_->RawMutex(), &absolute_time);
    if (status == ETIMEDOUT) {
      return false;
    }
    ASSERT(status == 0);
    return true;
  }

  void Signal() {
    int status = pthread_cond_signal(&cond_);
    ASSERT(status == 0);
  }

  // Wake all threads that are waiting on this condition.
  void Broadcast() {
    int status = pthread_cond_broadcast(&cond_);
    ASSERT(status == 0);
  }

 private:
  // Not copyable
  Condition(const Condition& other);
  Condition& operator=(const Condition& other);

  pthread_cond_t cond_;
  Mutex* mutex_;
};

}  // namespace peloton
