//
// Created by Rui Wang on 16-4-4.
//
#pragma once

#include <mutex>
#include <vector>
#include <thread>
#include <condition_variable>

namespace peloton {
namespace executor {

template <typename T>
class BlockingQueue {
public:
    T Get() {
      std::unique_lock<std::mutex> locker(lock_);
      while (storage_.size() == 0) {
        cond_.wait(locker);
      }

      T item = storage_.front();
      storage_.erase(storage_.begin());
      locker.unlock();
      return item;
    }

    void Put(const T& item) {
      std::unique_lock<std::mutex> locker(lock_);
      storage_.push_back(item);
      locker.unlock();
      cond_.notify_one();
    }

    void Put(const std::vector<T>& items) {
     std::unique_lock<std::mutex> locker(lock_);
     storage_.push_back(items);
     locker.unlock();
     cond_.notify_all();
    }

private:
    std::vector<T> storage_;
    std::mutex lock_;
    std::condition_variable cond_;
};

}  // namespace executor
}  // namespace peloton

