//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lockfree_spsc_queue.h
//
// Identification: src/backend/common/lockfree_spsc_queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/lockfree/spsc_queue.hpp>

namespace peloton {

//===--------------------------------------------------------------------===//
// Lockfree Spsc Queue
// this is a wrapper of boost lockfree spsc queue.
// this data structure supports single consumer and single producer.
//===--------------------------------------------------------------------===//

template <typename T>
class LockfreeSpscQueue {
 public:
  LockfreeSpscQueue(const size_t &size) : queue_(size) {}

  LockfreeSpscQueue(const LockfreeSpscQueue&) = delete;             // disable copying
  LockfreeSpscQueue& operator=(const LockfreeSpscQueue&) = delete;  // disable assignment

  // return true if pop is successful.
  // if queue is empty, then return false.
  bool Pop(T& item) {
    return queue_.pop(item);
  }

  // return true if push is successful.
  bool Push(const T& item) {
    return queue_.push(item);
  }

 private:
  boost::lockfree::spsc_queue<T> queue_;
};

}  // namespace peloton
