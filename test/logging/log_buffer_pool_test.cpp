//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_pool_test.cpp
//
// Identification: test/logging/log_buffer_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//#include "logging/log_buffer_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Pool Tests
//===--------------------------------------------------------------------===//

class LogBufferPoolTests : public PelotonTest {};

TEST_F(LogBufferPoolTests, PoolTest) {
  
  /*logging::LogBufferPool log_buffer_pool(1);

  size_t thread_id = log_buffer_pool.GetThreadId();

  EXPECT_EQ(thread_id, 1);

  std::unique_ptr<logging::LogBuffer> log_buffer(new logging::LogBuffer(1, 1));

  log_buffer_pool.GetBuffer(1);

  size_t slot_count = log_buffer_pool.GetEmptySlotCount();
  
  EXPECT_EQ(slot_count, log_buffer_pool.GetMaxSlotCount() - 1);

  log_buffer_pool.PutBuffer(std::move(log_buffer));

  slot_count = log_buffer_pool.GetEmptySlotCount();
  
  EXPECT_EQ(slot_count, log_buffer_pool.GetMaxSlotCount());
*/
}

}
}
