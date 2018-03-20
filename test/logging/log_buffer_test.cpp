//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_test.cpp
//
// Identification: test/logging/log_buffer_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/log_buffer.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Tests
//===--------------------------------------------------------------------===//

class LogBufferTests : public PelotonTest {};

TEST_F(LogBufferTests, LogBufferTest) {
  
  logging::LogBuffer log_buffer(1, 1);

  int eid = log_buffer.GetEpochId();

  EXPECT_EQ(eid, 1);

  int thread_id = log_buffer.GetThreadId();

  EXPECT_EQ(thread_id, 1);

  log_buffer.Reset();

  eid = log_buffer.GetEpochId();

  EXPECT_EQ(eid, INVALID_EID);

  bool rt = log_buffer.Empty();

  EXPECT_TRUE(rt);

  char *data = log_buffer.GetData();

  int num = 99;

  rt = log_buffer.WriteData((char*)(&num), sizeof(num));

  EXPECT_TRUE(rt);

  int num2;

  PL_MEMCPY(&num2, data, sizeof(num));

  EXPECT_EQ(num2, 99);

  size_t size = log_buffer.GetSize();

  EXPECT_EQ(size, sizeof(num));

  log_buffer.Reset();

  rt = log_buffer.Empty();

  EXPECT_TRUE(rt);

  size = log_buffer.GetSize();

  EXPECT_EQ(size, 0);
  
}

}
}
