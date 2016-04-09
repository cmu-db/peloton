//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// buffer_pool_test.cpp
//
// Identification: tests/logging/buffer_pool_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/logging/circular_buffer_pool.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Buffer Pool Tests
//===--------------------------------------------------------------------===//

class BufferPoolTests : public PelotonTest {};

void EnqueueTest(logging::CircularBufferPool *buffer_pool) {
  for (unsigned int i = 0; i < BUFFER_POOL_SIZE; i++) {
    std::shared_ptr<logging::LogBuffer> buf(new logging::LogBuffer());
    buf->SetSize(i);
    buffer_pool->Put(buf);
  }
}

void DequeueTest(logging::CircularBufferPool *buffer_pool) {
  for (unsigned int i = 0; i < BUFFER_POOL_SIZE; i++) {
    auto buf = buffer_pool->Get();
    assert(buf);
    EXPECT_EQ(buf->GetSize(), i);
  }
}

TEST_F(BufferPoolTests, BufferPoolBasicTest) {
  for (int i = 0; i < 30; i++) {
    logging::CircularBufferPool buffer_pool;
    std::thread enqueue_thread(EnqueueTest, &buffer_pool);
    std::thread dequeue_thread(DequeueTest, &buffer_pool);
    enqueue_thread.join();
    dequeue_thread.join();
  }
}

}  // End test namespace
}  // End peloton namespace
