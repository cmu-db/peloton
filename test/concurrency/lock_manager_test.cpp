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

/**
 * @brief Test basic functionality of lock manager
 */
TEST_F(LockManagerTests, FunctionalityTest) {
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  bool result0 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  bool result1 = lm->LockShared((oid_t)1);
  bool result2 = lm->UnlockShared((oid_t)1);
  bool result3 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result0);
  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
}

/**
 * @brief Test shared lock behaviour
 */
TEST_F(LockManagerTests, LockSharedTest) {
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  bool result0 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  bool result1 = lm->LockShared((oid_t)1);
  bool result2 = lm->LockShared((oid_t)1);
  bool result3 = lm->UnlockShared((oid_t)1);
  bool result4 = lm->UnlockShared((oid_t)1);
  bool result5 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result0);
  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
  EXPECT_TRUE(result4);
  EXPECT_TRUE(result5);
}

/**
 * @brief Test lock upgrade/downgrade behaviour
 */
TEST_F(LockManagerTests, LockChangeTest) {
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  bool result0 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  bool result1 = lm->LockShared((oid_t)1);
  bool result2 = lm->LockToExclusive((oid_t)1);
  bool result3 = lm->LockToShared((oid_t)1);
  bool result4 = lm->LockShared((oid_t)1);
  bool result5 = lm->UnlockShared((oid_t)1);
  bool result6 = lm->UnlockShared((oid_t)1);
  bool result7 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result0);
  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
  EXPECT_TRUE(result4);
  EXPECT_TRUE(result5);
  EXPECT_TRUE(result6);
  EXPECT_TRUE(result7);
}

/**
 * @brief Test double create
 */
TEST_F(LockManagerTests, DoubleCreateTest) {
  // Initialize lock
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  bool result0 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  // Initialize lock again
  bool result1 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  // Try to remove lock
  bool result2 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result0);
  EXPECT_FALSE(result1);
  EXPECT_TRUE(result2);
}

/**
 * @brief Test double remove
 */
TEST_F(LockManagerTests, DoubleRemoveTest) {
  // Initialize lock
  concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
  bool result0 = lm->InitLock((oid_t)1, concurrency::LockManager::RW_LOCK);
  // Try to remove lock
  bool result1 = lm->RemoveLock((oid_t)1);
  // Try to remove lock again
  bool result2 = lm->RemoveLock((oid_t)1);
  EXPECT_TRUE(result0);
  EXPECT_TRUE(result1);
  EXPECT_FALSE(result2);
}

}  // namespace test
}  // namespace peloton
