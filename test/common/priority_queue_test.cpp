//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// queue_test.cpp
//
// Identification: test/common/queue_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/priority_queue.h"

#include "common/harness.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// PriorityQueue Test
//===--------------------------------------------------------------------===//

class PriorityQueueTest : public PelotonTest {};

// Test basic functionality
TEST_F(PriorityQueueTest, BasicTest) {

  int num_elements = 100;
  uint32_t value;
  PriorityQueue<uint32_t> priority_queue(num_elements);

  priority_queue.Push(1);
  priority_queue.Push(2);
  priority_queue.Push(3);
  priority_queue.Push(4);

  EXPECT_EQ(priority_queue.GetSize(), 4);

  priority_queue.Pop(value);
  EXPECT_EQ(value, 4);

}

}  // End test namespace
}  // End peloton namespace
