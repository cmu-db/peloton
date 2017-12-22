//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/codegen/proxy/executor_context_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ExecutorContext, "executor::ExecutorContext", MEMBER(opaque));

// Define a method that proxies executor::ExecutorContext::GetTransaction()
DEFINE_METHOD(peloton::executor, ExecutorContext, GetTransaction);

}  // namespace codegen
}  // namespace peloton