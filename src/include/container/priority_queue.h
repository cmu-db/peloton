//===----------------------------------------------------------------------===//
//
//       Peloton
//
// queue.h
//
// Identification: src/include/common/queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "libcds/cds/container/mspriority_queue.h"

namespace peloton {

// PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
#define PRIORITY_QUEUE_TEMPLATE_ARGUMENTS template <typename ValueType>

// PRIORITY_QUEUE_TYPE
#define PRIORITY_QUEUE_TYPE PriorityQueue<ValueType>

PRIORITY_QUEUE_TEMPLATE_ARGUMENTS
class PriorityQueue {

 public:
  PriorityQueue() = delete;
  PriorityQueue(PriorityQueue const &) = delete;

  PriorityQueue(const size_t capacity);
  ~PriorityQueue();

  // Inserts a item into priority queue.
  bool Push(const ValueType v);

  // Extracts item with high priority.
  bool Pop(ValueType &v);

  // Clears the queue (not atomic)
  void Clear();

  // Checks is the priority queue is empty.
  bool IsEmpty() const;

  // Checks if the priority queue is full.
  bool IsFull() const;

  // Return capacity of the priority queue.
  size_t  GetCapacity() const;

  // Returns current size of priority queue.
  size_t GetSize() const;

 private:

  // Michael & Scott array-based lock-based concurrent priority queue heap
  // http://libcds.sourceforge.net/doc/cds-api/
  typedef cds::container::MSPriorityQueue<ValueType>  pq_t;

  pq_t priority_queue;
};

}  // namespace peloton
