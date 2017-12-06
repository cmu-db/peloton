//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down.h
//
// Identification: src/include/codegen/multi_thread/count_down.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

#include <mutex>
#include <condition_variable>

namespace peloton {
namespace codegen {

class CountDown {
 public:
  // "Constructor".
  void Init(int32_t count);

  // "Destructor".
  void Destroy();

  // Decrement the count by 1.
  void Decrease();

  // Wait until the count becomes 0.
  void Wait();

 private:
  // Use Init and Destroy on allocated memory instead.
  explicit CountDown(int32_t count) : count_(count) {}
  ~CountDown() = default;

  std::int32_t count_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace codegen
}  // namespace peloton
