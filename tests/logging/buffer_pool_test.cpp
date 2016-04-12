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
#include "logging/logging_tests_util.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Buffer Pool Tests
//===--------------------------------------------------------------------===//

class BufferPoolTests : public PelotonTest {};

void EnqueueTest(logging::CircularBufferPool *buffer_pool, unsigned int count) {
  for (unsigned int i = 0; i < count; i++) {
    std::unique_ptr<logging::LogBuffer> buf(new logging::LogBuffer(0));
    buf->SetSize(i);
    buffer_pool->Put(std::move(buf));
  }
}

void DequeueTest(logging::CircularBufferPool *buffer_pool, unsigned int count) {
  for (unsigned int i = 0; i < count; i++) {
    auto buf = std::move(buffer_pool->Get());
    assert(buf);
    EXPECT_EQ(buf->GetSize(), i);
  }
}

TEST_F(BufferPoolTests, BufferPoolBasicTest) {
  logging::CircularBufferPool buffer_pool0;
  EnqueueTest(&buffer_pool0, 5);
  EXPECT_EQ(buffer_pool0.GetSize(), 5);

  DequeueTest(&buffer_pool0, 5);
  EXPECT_EQ(buffer_pool0.GetSize(), 0);

  EnqueueTest(&buffer_pool0, BUFFER_POOL_SIZE);
  EXPECT_EQ(buffer_pool0.GetSize(), BUFFER_POOL_SIZE);

  for (int i = 0; i < 10; i++) {
    logging::CircularBufferPool buffer_pool1;
    std::thread enqueue_thread(EnqueueTest, &buffer_pool1, BUFFER_POOL_SIZE);
    std::thread dequeue_thread(DequeueTest, &buffer_pool1, BUFFER_POOL_SIZE);
    enqueue_thread.join();
    dequeue_thread.join();
  }
}

TEST_F(BufferPoolTests, LogBufferBasicTest) {
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  std::unique_ptr<storage::DataTable> recovery_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));

  // prepare tuples
  auto mutate = true;
  auto random = false;
  int num_rows = tile_group_size * table_tile_group_count;
  std::vector<std::shared_ptr<storage::Tuple>> tuples =
      LoggingTestsUtil::BuildTuples(recovery_table.get(), num_rows, mutate,
                                    random);
  std::vector<logging::TupleRecord> records =
      LoggingTestsUtil::BuildTupleRecords(tuples, tile_group_size,
                                          table_tile_group_count);
  logging::LogBuffer log_buffer(0);
  size_t total_length = 0;
  for (auto record : records) {
    assert(record.GetTuple()->GetSchema());
    CopySerializeOutput output_buffer;
    record.Serialize(output_buffer);
    size_t len = record.GetMessageLength();
    total_length += len;
    log_buffer.WriteRecord(&record);
  }
  EXPECT_EQ(log_buffer.GetSize(), total_length);
}

}  // End test namespace
}  // End peloton namespace
