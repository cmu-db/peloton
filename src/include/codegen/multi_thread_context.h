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

#include <atomic>

#include "codegen/codegen.h"
#include "codegen/code_context.h"
#include "codegen/function_builder.h"
#include "codegen/type.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  MultiThreadContext(uint64_t thread_id, uint64_t thread_count)
   : thread_id_(thread_id), thread_count_(thread_count) {}

  uint64_t GetRangeStart(uint64_t thread_id, uint64_t tile_group_num) {
    // TODO(tq5124): fix this.
	  return thread_id + tile_group_num;
  }

  uint64_t GetRangeEnd(uint64_t thread_id, uint64_t tile_group_num) {
    // TODO(tq5124): fix this.
    return thread_id + tile_group_num;
  }

 private:

  uint64_t thread_id_;
  uint64_t thread_count_;
};

}
}
