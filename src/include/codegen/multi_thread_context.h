//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_thread_context.h
//
// Identification: src/include/codegen/multi_thread_context.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  static MultiThreadContext GetInstance(int64_t thread_id, int64_t thread_count);

  int64_t GetRangeStart(int64_t tile_group_num);

  int64_t GetRangeEnd(int64_t tile_group_num);

 private:

  MultiThreadContext(int64_t thread_id, int64_t thread_count)
     : thread_id_(thread_id), thread_count_(thread_count) {}

  int64_t thread_id_;
  int64_t thread_count_;
};

}
}
