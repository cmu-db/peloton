//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <map>
#include <utility>

#include <pthread.h>
#include <concurrency/lock_manager.h>
#include "common/logger.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Lock manager
//===--------------------------------------------------------------------===//

// Get the global variable instance of lock manager
LockManager *LockManager::GetInstance() {
  static LockManager global_lm;
  return &global_lm;
}

// Initialize lock for given oid
bool LockManager::InitLock(oid_t oid, LockManager::LockType /*type*/){
  // Initialize new lock for the object
  pthread_rwlock_t rw_lock;
  pthread_rwlock_init(&rw_lock, nullptr);

  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_wrlock(&internal_rw_lock_);

  // Insert lock into map
  lock_map_.insert(std::make_pair(oid, rw_lock));

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  LOG_WARN("INIT LOCK SUCCSS!!%u", oid);
  return true;
}

// Remove the lock specified by oid from data structure
bool LockManager::RemoveLock(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_wrlock(&internal_rw_lock_);
  LOG_WARN("REMOVE LOCK!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Remove from the lock map
  lock_map_.erase(oid);


  // Remove the lock, unlock internal lock
  pthread_rwlock_destroy(rw_lock);
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

// Acquire shared lock for RW_LOCK(blocking)
bool LockManager::LockShared(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_rdlock(&internal_rw_lock_);
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Read (shared) lock
  pthread_rwlock_rdlock(rw_lock);

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

// Acquire exclusive lock for RW_LOCK(blocking)
bool LockManager::LockExclusive(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_rdlock(&internal_rw_lock_);
  LOG_WARN("LOCK E!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Write (exclusive) lock
  pthread_rwlock_wrlock(rw_lock);

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

// Unlock and lock to shared for RW_LOCK(blocking)
bool LockManager::LockToShared(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_rdlock(&internal_rw_lock_);
  LOG_WARN("LOCK TS!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Unlock first, then lock to shared
  pthread_rwlock_unlock(rw_lock);
  pthread_rwlock_rdlock(rw_lock);

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

// Unlock and lock to exclusive for RW_LOCK(blocking)
bool LockManager::LockToExclusive(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_rdlock(&internal_rw_lock_);
  LOG_WARN("LOCK TE!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Unlock first, then lock to shared
  pthread_rwlock_unlock(rw_lock);
  pthread_rwlock_wrlock(rw_lock);

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

// Unlock RW lock
bool LockManager::UnlockRW(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  pthread_rwlock_rdlock(&internal_rw_lock_);
  LOG_WARN("UNLOCK!!%u", oid);

  // Try to access the lock
  pthread_rwlock_t *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    pthread_rwlock_unlock(&internal_rw_lock_);
    return false;
  }

  // Unlock
  pthread_rwlock_unlock(rw_lock);

  // Unlock internal lock
  pthread_rwlock_unlock(&internal_rw_lock_);
  return true;
}

}  // namespace concurrency
}  // namespace peloton
