//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// readwrite_latch.h
//
// Identification: src/include/common/synchronization/readwrite_latch.h
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
// Read-Write Latch
//===--------------------------------------------------------------------===//

namespace peloton {
namespace common {
namespace synchronization {

// Wrapper around pthread_rwlock_t

class ReadWriteLatch {
 public:
  ReadWriteLatch(ReadWriteLatch const &) = delete;
  ReadWriteLatch &operator=(ReadWriteLatch const &) = delete;

  ReadWriteLatch() { pthread_rwlock_init(&rw_lock_, nullptr); }

  ~ReadWriteLatch() { pthread_rwlock_destroy(&rw_lock_); }

  void ReadLock() const { pthread_rwlock_rdlock(&rw_lock_); }

  void WriteLock() const { pthread_rwlock_wrlock(&rw_lock_); }

  void Unlock() const { pthread_rwlock_unlock(&rw_lock_); }

 private:
  // can only be moved, not copied
  mutable pthread_rwlock_t rw_lock_;
};

}  // namespace synchronization
}  // namespace common
}  // namespace peloton