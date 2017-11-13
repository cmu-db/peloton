//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_thread_pool_proxy.h
//
// Identification: src/include/codegen/proxy/executor_thread_pool_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/multi_thread/executor_thread_pool.h"
#include "codegen/proxy/task_info_proxy.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(ExecutorThreadPool) {
  DECLARE_MEMBER(0, char[sizeof(ExecutorThreadPool)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(GetInstance);
  DECLARE_METHOD(GetNumThreads);
  DECLARE_METHOD(SubmitTask);
};

TYPE_BUILDER(ExecutorThreadPool, codegen::ExecutorThreadPool);

}  // namespace codegen
}  // namespace peloton
