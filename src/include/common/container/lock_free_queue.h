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
//
// Lock-free Queue -- Supports multiple consumers and multiple producers.
//
//===--------------------------------------------------------------------===//
template <typename T>
class LockFreeQueue {
 public:
  /// Constructor
  explicit LockFreeQueue(size_t size) : queue_(size) {}

  /// This class cannot be copied
  DISALLOW_COPY(LockFreeQueue);

  /**
   * @brief Move-enqueues the given item.
   * @param item To be enqueued
   */
  void Enqueue(T &&item) { queue_.enqueue(std::move(item)); }

  /**
   * @brief Enqueues the given item by copying it into this queue
   * @param item To be enqueued
   */
  void Enqueue(const T &item) { queue_.enqueue(item); }

  /**
   * @brief Tries to dequeue one item
   * @param[out] item That is dequeued
   * @return True is an item was removed, false if the queue was empty
   */
  bool Dequeue(T &item) { return queue_.try_dequeue(item); }

  /**
   * @brief Determines if the queue is empty
   * @return True if the queue is empty
   */
  bool IsEmpty() const { return queue_.size_approx() == 0; }

 private:
  // Underlying moodycamel concurrent queue
  moodycamel::ConcurrentQueue<T> queue_;
};

}  // namespace peloton
