//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <map>
#include <utility>

#include <pthread.h>
#include <common/internal_types.h>
#include "common/logger.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Lock manager
//===--------------------------------------------------------------------===//

class LockManager {
public:

  // Type of locks. Currently only supports RW_LOCK
  enum LockType{
    LOCK,
    RW_LOCK,
    SPIN_LOCK
  };

  // Constructor
  LockManager() {pthread_rwlock_init(&internal_rw_lock_, nullptr);}

  // Destructor
  virtual ~LockManager() {pthread_rwlock_destroy(&internal_rw_lock_);}

  // Initialize lock for given oid
  bool InitLock(oid_t oid, LockType type);

  // Remove the lock specified by oid from data structure
  bool RemoveLock(oid_t oid);

  // Acquire shared lock for RW_LOCK(blocking)
  bool LockShared(oid_t oid);

  // Acquire exclusive lock for RW_LOCK(blocking)
  bool LockExclusive(oid_t oid);

  // Unlock and lock to shared for RW_LOCK(blocking)
  bool LockToShared(oid_t oid);

  // Unlock and lock to exclusive for RW_LOCK(blocking)
  bool LockToExclusive(oid_t oid);

  // Unlock RW lock
  bool UnlockRW(oid_t oid);

  // Return the global variable instance
  static LockManager* GetInstance();

private:

  // Local RW lock to protect lock dict
  pthread_rwlock_t internal_rw_lock_;

  // Map to store RW_LOCK for different objects
  std::map<oid_t, pthread_rwlock_t> lock_map_;

  // Get RW lock by oid
  pthread_rwlock_t *GetLock(oid_t oid){
    // Try to access the lock
    pthread_rwlock_t *rw_lock;
    try{
      rw_lock = &(lock_map_.at(oid));
      LOG_WARN("GET LOCK SUCCSS!!");
    }
    catch(const std::out_of_range& oor) {
      LOG_WARN("GET LOCK FAILED!! %u", oid);
      return nullptr;
    }
    return rw_lock;
  }

};
}  // namespace concurrency
}  // namespace peloton
