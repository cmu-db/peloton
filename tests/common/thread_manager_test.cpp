//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_manager_test.cpp
//
// Identification: tests/common/thread_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/common/thread_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Thread Manager Test
//===--------------------------------------------------------------------===//

class ThreadManagerTests : public PelotonTest {};

/* this is the old test
TEST_F(ThreadManagerTests, BasicTest) {
  auto& thread_manager = ThreadManager::GetInstance();
  std::shared_ptr<std::thread> t1(new std::thread);
  bool status = thread_manager.AttachThread(t1);
  EXPECT_EQ(status, true);
  status = thread_manager.AttachThread(t1);
  EXPECT_EQ(status, false);
  status = thread_manager.DetachThread(t1);
  EXPECT_EQ(status, true);
  status = thread_manager.DetachThread(t1);
  EXPECT_EQ(status, false);
}
*/

TEST_F(ThreadManagerTests, BasicTest1) {
  auto& thread_manager1 = ThreadManager::GetInstance();
  auto& thread_manager2 = ThreadManager::GetInstance();

  bool status = (&thread_manager1 == &thread_manager2);
  EXPECT_EQ(status, true);
}

}  // End test namespace
}  // End peloton namespace
