//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// message_queue.h
//
// Identification: src/backend/networking/message_queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace peloton {
namespace networking {

template <typename T>
class MessageQueue {
 public:
  MessageQueue() = default;
  MessageQueue(const MessageQueue&) = delete;             // disable copying
  MessageQueue& operator=(const MessageQueue&) = delete;  // disable assignment

  T Pop() {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) {
      cond_.wait(mlock);
    }
    auto val = queue_.front();
    queue_.pop();
    return val;
  }

  void Pop(T& item) {
    std::unique_lock<std::mutex> mlock(mutex_);

    while (queue_.empty()) {
      cond_.wait(mlock);
    }

    item = queue_.front();
    queue_.pop();
  }

  void Push(const T& item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(item);
    mlock.unlock();
    cond_.notify_one();
  }

 private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

}  // namespace networking
}  // namespace peloton
