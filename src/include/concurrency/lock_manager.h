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

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Lock manager
//===--------------------------------------------------------------------===//

class LockManager {
 public:

  //===--------------------------------------------------------------------===//
  // Safe lock
  //===--------------------------------------------------------------------===//
  // Initial one of these after initialed lock on oid
  // Remember to initial it as local variable
  // It will automatically unlock the corresponding lock when the current scope
  // exits (since the unlock logic is in its destructor).
  class SafeLock {
   public:
    // Type of locked read write locks.
    enum RWLockType { SHARED, EXCLUSIVE, INVALID };

    // Default Constructor
    SafeLock(){
      oid_ = INVALID_OID;
      type_ = RWLockType::INVALID;
    };

    SafeLock(oid_t oid, RWLockType type){
      oid_ = oid;
      type_ = type;
    }

    // Set the oid and type for the lock
    void Set(oid_t oid, RWLockType type){
      oid_ = oid;
      type_ = type;
    }

    // Unlock the given lock in the constructor
    ~SafeLock(){
      LockManager *lm = LockManager::GetInstance();
      if (type_ == RWLockType::EXCLUSIVE){
        lm->UnlockExclusive(oid_);
      }
      else if (type_ == RWLockType::SHARED){
        lm->UnlockShared(oid_);
      }
    }

   // Members
   private:
    oid_t oid_;
    RWLockType type_;
  };

  // Type of locks. Currently only support RWLOCK
  enum LockType { RW_LOCK };

  // Type of locked read write locks.
  enum RWLockType { SHARED, EXCLUSIVE, INVALID };

  // This is used by transaction manager to properly release locks.
  struct LockWithOid{
    oid_t oid;
    RWLockType type;
  };

  // Constructor
  LockManager() {}

  // Destructor
  ~LockManager() {
    // Iterate through mapped locks
    std::vector<oid_t> v;
    auto itr = lock_map_.begin();
    for (; itr != lock_map_.end(); itr++){
      v.push_back(itr->first);
    }

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
  // Changed to shared_ptr for safety reasons
  std::map<oid_t, std::shared_ptr<boost::upgrade_mutex> > lock_map_;

  // Get RW lock by oid
  boost::upgrade_mutex *GetLock(oid_t oid) {
    // Try to access the lock
    boost::upgrade_mutex *rw_lock;
    try {
      rw_lock = lock_map_.at(oid).get();
    } catch (const std::out_of_range &oor) {
      return nullptr;
    }
    return rw_lock;
  }
};
}  // namespace concurrency
}  // namespace peloton
