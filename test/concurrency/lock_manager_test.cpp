//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_manager_test.cpp
//
// Identification: test/concurrency/lock_manager_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/lock_manager.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Lock Manager functionality tests
//===--------------------------------------------------------------------===//

class LockManagerTests : public PelotonTest {};

TEST_F(LockManagerTests, FunctionalityTest){
  // Initialize lock
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  // Try to lock shared
  bool result1 = lm->LockShared((oid_t)1);
  // Try to unlock
  bool result2 = lm->UnlockRW((oid_t)1);
  // Try to remove lock
  bool result3 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result1 && result2 && result3);
}

TEST_F(LockManagerTests, LockSharedTest){
  // Initialize lock
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);

  // Try to lock shared
  bool result1 = lm->LockShared((oid_t)1);
  // Try to lock shared
  bool result2 = lm->LockShared((oid_t)1);
  // Try to lock shared
  bool result3 = lm->UnlockRW((oid_t)1);
  // Try to lock shared
  bool result4 = lm->UnlockRW((oid_t)1);
  // Try to remove lock
  bool result5 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result1 && result2 && result3 && result4 && result5);
}

TEST_F(LockManagerTests, LockChangeTest){
  // Initialize lock
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);

  // Try to lock shared
  bool result1 = lm->LockShared((oid_t)1);
  // Try to lock change to exclusive
  bool result2 = lm->LockToExclusive((oid_t)1);
  // Try to lock change to shared
  bool result3 = lm->LockToShared((oid_t)1);
  // Try to lock shared
  bool result4 = lm->LockToShared((oid_t)1);
  // Try to lock shared
  bool result5 = lm->UnlockRW((oid_t)1);
  // Try to lock shared
  bool result6 = lm->UnlockRW((oid_t)1);
  // Try to remove lock
  bool result7 = lm->RemoveLock((oid_t)1);
  // Try to remove lock
  bool result8 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result1 && result2 && result3 && result4 && result5 && result6 && result7 &&result8);
}

}  // namespace test
}  // namespace peloton



