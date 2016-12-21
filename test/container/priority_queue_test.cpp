//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// priority_queue_test.cpp
//
// Identification: test/container/priority_queue_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "container/priority_queue.h"

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
  EXPECT_EQ(priority_queue.IsFull(), false);

  priority_queue.Pop(value);
  EXPECT_EQ(value, 4);
  priority_queue.Clear();
  EXPECT_EQ(priority_queue.GetSize(), 0);
  EXPECT_EQ(priority_queue.IsEmpty(), true);
  for (uint32_t i = 0; i < (uint32_t)priority_queue.GetCapacity() - 1; ++i) {
    priority_queue.Push(i);
    EXPECT_EQ(priority_queue.GetSize(), i + 1);
    EXPECT_EQ(priority_queue.IsEmpty(), false);
    EXPECT_EQ(priority_queue.IsFull(), false);
  }
  priority_queue.Push(priority_queue.GetCapacity() - 1);
  EXPECT_EQ(priority_queue.GetSize(), priority_queue.GetCapacity());
  EXPECT_EQ(priority_queue.IsEmpty(), false);
  EXPECT_EQ(priority_queue.IsFull(), true);

}

}  // End test namespace
}  // End peloton namespace
