//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lockfree_queue.h
//
// Identification: src/backend/common/lockfree_queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrentqueue/concurrentqueue.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Lockfree Queue
// Supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

template <typename T>
class LockfreeQueue {
 public:
  LockfreeQueue(const size_t &size) : queue_(size) {}

  LockfreeQueue(const LockfreeQueue&) = delete;             // disable copying
  LockfreeQueue& operator=(const LockfreeQueue&) = delete;  // disable assignment

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
    return false;
    //return queue_.is_empty();
  }

 private:

  // Underlying moodycamel's concurrent queue
  moodycamel::ConcurrentQueue<T> queue_;
};

}  // namespace peloton
