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

#include "concurrentqueue/concurrentqueue.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Lock-free Queue -- Supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

template <typename T>
class LockFreeQueue {
 public:
  LockFreeQueue(const size_t &size) : queue_(size) {}

  LockFreeQueue(const LockFreeQueue&) = delete;             // disable copying
  LockFreeQueue& operator=(const LockFreeQueue&) = delete;  // disable assignment

  // Enqueues one item, allocating extra space if necessary
  void Enqueue(T& item) {
    queue_.enqueue(item);
  }

  void Enqueue(const T& item) {
    queue_.enqueue(item);
  }

  // Dequeues one item, returning true if an item was found
  // or false if the queue appeared empty
  bool Dequeue(T& item) {
    return queue_.try_dequeue(item);
  }

  bool Dequeue(const T& item) {
    return queue_.try_dequeue(item);
  }

  bool IsEmpty() {
    return queue_.size_approx() == 0;
  }

 private:

  // Underlying moodycamel's concurrent queue
  moodycamel::ConcurrentQueue<T> queue_;
};

}  // namespace peloton
