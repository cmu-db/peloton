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

#include <boost/thread/shared_mutex.hpp>
#include "concurrency/lock_manager.h"
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
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock();

  // Try to find the lock in table
  boost::upgrade_mutex *lock = GetLock(oid);
  if (lock != nullptr){
    internal_rw_lock_.unlock();
    return false;
  }

  // Initialize new lock for the object
  boost::upgrade_mutex *rw_lock = new boost::upgrade_mutex();

  std::pair<oid_t, boost::upgrade_mutex*> p;
  p.first = oid;
  p.second = rw_lock;
  // Insert lock into map
  lock_map_.insert(p);

  // Unlock internal lock
  internal_rw_lock_.unlock();
  LOG_WARN("INIT LOCK SUCCSS!!%u", oid);
  return true;
}

// Remove the lock specified by oid from data structure
bool LockManager::RemoveLock(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock();
  LOG_WARN("REMOVE LOCK!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock();
    return false;
  }

  // Remove from the lock map
  lock_map_.erase(oid);
  delete(rw_lock);

  // Remove the lock, unlock internal lock
  internal_rw_lock_.unlock();
  return true;
}

// Acquire shared lock for RW_LOCK(blocking)
bool LockManager::LockShared(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // Read (shared) lock
  rw_lock->lock_shared();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

// Acquire exclusive lock for RW_LOCK(blocking)
bool LockManager::LockExclusive(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // Read (shared) lock
  rw_lock->lock();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

// Unlock and lock to shared for RW_LOCK(blocking)
bool LockManager::LockToShared(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // Read (shared) lock
  rw_lock->unlock_and_lock_shared();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

// Unlock and lock to exclusive for RW_LOCK(blocking)
bool LockManager::LockToExclusive(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // Read (shared) lock
  rw_lock->unlock_shared();
  rw_lock->lock();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

// Unlock shared lock
bool LockManager::UnlockShared(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // unlock shared lock
  rw_lock->unlock_shared();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

// Unlock exclusive lock
bool LockManager::UnlockExclusive(oid_t oid){
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  LOG_WARN("LOCK S!!%u", oid);

  // Try to access the lock
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr){
    internal_rw_lock_.unlock_shared();
    return false;
  }

  // unlock shared lock
  rw_lock->unlock();

  LOG_WARN("LOCKed Ssss!!%u", oid);

  // Unlock internal lock
  internal_rw_lock_.unlock_shared();
  return true;
}

}  // namespace concurrency
}  // namespace peloton
