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

#include <boost/lockfree/queue.hpp>

namespace peloton {

//===--------------------------------------------------------------------===//
// Lockfree Queue
// this is a wrapper of boost lockfree queue.
// this data structure supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

template <typename T>
class LockfreeQueue {
 public:
  LockfreeQueue(const size_t &size) : queue_(size) {}

  LockfreeQueue(const LockfreeQueue&) = delete;             // disable copying
  LockfreeQueue& operator=(const LockfreeQueue&) = delete;  // disable assignment

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
  boost::lockfree::queue<T> queue_;
};

}  // namespace peloton
