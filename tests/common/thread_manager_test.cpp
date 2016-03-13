//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache_test.cpp
//
// Identification: tests/common/cache_test.cpp
//
// Copyright (c) 201CACHE_SIZE, Carnegie Mellon University Database Group
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

TEST_F(ThreadManagerTests, BasicTest) {

  auto& server_thread_manager1 = ThreadManager::GetServerThreadPool();
  auto& server_thread_manager2 = ThreadManager::GetServerThreadPool();

  bool status = (&server_thread_manager1 == &server_thread_manager2);
  EXPECT_EQ(status, true);

  auto& client_thread_manager1 = ThreadManager::GetClientThreadPool();
  auto& client_thread_manager2 = ThreadManager::GetClientThreadPool();

  status = (&client_thread_manager1 == &client_thread_manager2);
  EXPECT_EQ(status, true);

  status = (&server_thread_manager1 == &client_thread_manager2);
  EXPECT_EQ(status, false);

}

}  // End test namespace
}  // End peloton namespace
