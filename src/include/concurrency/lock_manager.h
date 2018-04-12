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

#include "common/internal_types.h"
#include "common/logger.h"
#include <boost/thread/shared_mutex.hpp>
#include <boost/foreach.hpp>

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Lock manager
//===--------------------------------------------------------------------===//

class LockManager {
 public:
  // Type of locks. Currently only supports RW_LOCK
  enum LockType { LOCK, RW_LOCK, SPIN_LOCK };

  // Constructor
  LockManager() {}

  // Destructor
  ~LockManager() {
    // Iterate through mapped locks
    std::pair<oid_t, boost::upgrade_mutex *> tmp;
    std::vector<oid_t> v;
    BOOST_FOREACH (tmp, lock_map_) { v.push_back(tmp.first); }

    // Remove each lock
    for (std::vector<oid_t>::iterator itr = v.begin(); itr != v.end(); itr++) {
      RemoveLock(*itr);
    }
    lock_map_.clear();
  }

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

  // Unlock Shared lock
  bool UnlockShared(oid_t oid);

  // Unlock Exclusive lock
  bool UnlockExclusive(oid_t oid);

  // Return the global variable instance
  static LockManager *GetInstance();

 private:
  // Local RW lock to protect lock dict
  boost::upgrade_mutex internal_rw_lock_;

  // Map to store RW_LOCK for different objects
  std::map<oid_t, boost::upgrade_mutex *> lock_map_;

  // Get RW lock by oid
  boost::upgrade_mutex *GetLock(oid_t oid) {
    // Try to access the lock
    boost::upgrade_mutex *rw_lock;
    try {
      rw_lock = lock_map_.at(oid);
    } catch (const std::out_of_range &oor) {
      return nullptr;
    }
    return rw_lock;
  }
};
}  // namespace concurrency
}  // namespace peloton
