//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_thread_pool_proxy.cpp
//
// Identification: src/codegen/proxy/executor_thread_pool_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/executor_thread_pool_proxy.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ExecutorThreadPool, "codegen::ExecutorThreadPool", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, ExecutorThreadPool, GetInstance);

DEFINE_METHOD(peloton::codegen, ExecutorThreadPool, GetNumThreads);

DEFINE_METHOD(peloton::codegen, ExecutorThreadPool, SubmitTask);

}  // namespace codegen
}  // namespace peloton
