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

#include <boost/thread/shared_mutex.hpp>
#include "concurrency/lock_manager.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Lock manager
//===--------------------------------------------------------------------===//

/**
 * @brief    Get the global variable instance of lock manager
 * @return   Pointer to the global lock manager instance
 */
LockManager *LockManager::GetInstance() {
  static LockManager global_lm;
  return &global_lm;
}

/**
 * @brief    Initialize lock for given oid
 * @details  Initialize lock for given oid. Currently,
 * only supports read/write lock
 * @param    oid  The oid of data table
 * @param    type The type of lock to initialize
 * @return   bool true if successful, false if not
 */
bool LockManager::InitLock(oid_t oid, LockManager::LockType /*type*/) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock();

  // Try to find the lock in table
  boost::upgrade_mutex *lock = GetLock(oid);
  if (lock != nullptr) {
    internal_rw_lock_.unlock();
    LOG_TRACE("Init lock failed, lock oid is %u.", oid);
    return false;
  }

  // Initialize new lock for the object
  std::pair<oid_t, std::shared_ptr<boost::upgrade_mutex> > p;
  p.first = oid;
  p.second = std::make_shared<boost::upgrade_mutex>();
  // Insert lock into map
  lock_map_.insert(p);

  // Unlock internal lock
  internal_rw_lock_.unlock();
  LOG_TRACE("Init lock success.");
  return true;
}

/**
 * @brief    Remove the lock specified by oid from data structure
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::RemoveLock(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock();
    LOG_TRACE("Remove lock failed, lock oid is %u.", oid);
    return false;
  }
  lock_map_.erase(oid);
  internal_rw_lock_.unlock();
  LOG_TRACE("Remove lock success.");
  return true;
}

/**
 * @brief    Acquire shared lock for RW_LOCK(blocking)
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::LockShared(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_TRACE("Shared lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->lock_shared();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Shared lock success.");
  return true;
}

/**
 * @brief    Acquire exclusive lock for RW_LOCK(blocking)
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::LockExclusive(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_TRACE("Exclusive lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->lock();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Exclusive lock success.");
  return true;
}

/**
 * @brief    Acquire exclusive lock for RW_LOCK(blocking)
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::LockToShared(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_TRACE("Change to shared lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->unlock_and_lock_shared();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Change to shared lock success.");
  return true;
}

/**
 * @brief    Unlock and lock to exclusive for RW_LOCK(blocking)
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::LockToExclusive(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_TRACE("Change to exclusive lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->unlock_shared();
  rw_lock->lock();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Change to exclusive lock success.");
  return true;
}

/**
 * @brief    Unlock shared lock
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::UnlockShared(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_DEBUG("Unlock shared lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->unlock_shared();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Unlock shared lock success.");
  return true;
}

/**
 * @brief    Unlock exclusive lock
 * @param    oid  The oid of data table
 * @return   bool true if successful, false if not
 */
bool LockManager::UnlockExclusive(oid_t oid) {
  // Need to lock internal lock to protect internal lock map
  internal_rw_lock_.lock_shared();
  boost::upgrade_mutex *rw_lock = GetLock(oid);
  if (rw_lock == nullptr) {
    internal_rw_lock_.unlock_shared();
    LOG_DEBUG("Unlock exclusive lock failed, lock oid is %u.", oid);
    return false;
  }
  rw_lock->unlock();
  internal_rw_lock_.unlock_shared();
  LOG_TRACE("Unlock exclusive lock success.");
  return true;
}

}  // namespace concurrency
}  // namespace peloton
