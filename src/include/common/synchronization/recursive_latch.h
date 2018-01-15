//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// recursive_latch.h
//
// Identification: src/include/common/synchronization/recursive_latch.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <pthread.h>

#include "common/platform.h"
#include "common/macros.h"

//===--------------------------------------------------------------------===//
// Recursive Latch
//===--------------------------------------------------------------------===//

namespace peloton {
namespace common {
namespace synchronization {

class RecursiveLatch {
 public:
  RecursiveLatch(RecursiveLatch const &) = delete;
  RecursiveLatch &operator=(RecursiveLatch const &) = delete;

  RecursiveLatch() {
    pthread_mutexmutexattr__init(&mutexattr_);
    pthread_mutexmutexattr__settype(&mutexattr_, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex_, &mutexattr_);
  }

  ~RecursiveLatch() { pthread_mutex_destroy(&mutex_); }

  void Lock() const { pthread_mutex_lock(&mutex_); }

  void Unlock() const { pthread_mutex_unlock(&mutex_); }

 private:
  // can only be moved, not copied
  pthread_mutexmutexattr__t mutexattr_;
  mutable pthread_mutex_t mutex_;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton