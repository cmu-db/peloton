//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// queue.h
//
// Identification: src/include/container/queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrentqueue/blockingconcurrentqueue.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Queue -- Supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

// QUEUE_TEMPLATE_ARGUMENTS
#define QUEUE_TEMPLATE_ARGUMENTS template <typename ValueType>

// QUEUE_MAP_TYPE
#define QUEUE_TYPE Queue<ValueType>

QUEUE_TEMPLATE_ARGUMENTS
class Queue {
 public:
  Queue() = delete;
  Queue(const Queue&) = delete;  // disable copying
  Queue& operator=(const Queue&) = delete;  // disable assignment

  Queue(const size_t& size);

  // Enqueues one item, allocating extra space if necessary
  void Enqueue(ValueType& item);

  // Dequeues one item, returning true if an item was found
  // or false if the queue appeared empty
  bool Dequeue(ValueType& item);

 private:

  // Underlying moodycamel's concurrent queue
  moodycamel::BlockingConcurrentQueue<ValueType> queue_;
};

}  // namespace peloton
