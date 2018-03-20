//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_free_queue.h
//
// Identification: src/include/container/lockfree_queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "concurrentqueue/concurrentqueue.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Lock-free Queue -- Supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

template <typename T>
class LockFreeQueue {
 public:
  explicit LockFreeQueue(const size_t &size) : queue_(size) {}

  DISALLOW_COPY(LockFreeQueue);

  void Enqueue(T &&item) { queue_.enqueue(std::move(item)); }

  void Enqueue(const T &item) { queue_.enqueue(item); }

  // Dequeues one item, returning true if an item was found
  // or false if the queue appeared empty
  bool Dequeue(T &item) { return queue_.try_dequeue(item); }

  bool IsEmpty() { return queue_.size_approx() == 0; }

 private:
  // Underlying moodycamel's concurrent queue
  moodycamel::ConcurrentQueue<T> queue_;
};

}  // namespace peloton
