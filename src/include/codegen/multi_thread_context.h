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
#include "codegen/code_context.h"
#include "codegen/function_builder.h"
#include "codegen/type.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  static MultiThreadContext GetInstance(int64_t thread_id, int64_t thread_count) {
    std::cout << "Constructing MTC with "
            << thread_id << "," << thread_count << std::endl;
    return MultiThreadContext(thread_id, thread_count);
  }

  int64_t GetRangeStart(int64_t tile_group_num) {
    int64_t slice_size = tile_group_num / thread_count_;
	  int64_t start = thread_id_ * slice_size;

	  std::cout << "Get start," << thread_id_ << "," << thread_count_ << "," << tile_group_num << "->" << start << std::endl;
	  return start;
  }

  int64_t GetRangeEnd(int64_t tile_group_num) {
    int64_t slice_size = tile_group_num / thread_count_;
    int64_t end = std::min(tile_group_num, (thread_id_ + 1) * slice_size);

    std::cout << "Get end," << thread_id_ << "," << thread_count_ << "," << tile_group_num << "->" << end << std::endl;
    return end;
  }

 private:

  MultiThreadContext(int64_t thread_id, int64_t thread_count)
     : thread_id_(thread_id), thread_count_(thread_count) {}

  int64_t thread_id_;
  int64_t thread_count_;
};

}
}
