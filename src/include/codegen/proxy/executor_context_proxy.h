//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/include/codegen/proxy/executor_context_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {

PROXY(ExecutorContext) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(executor::ExecutorContext)], opaque);
  DECLARE_TYPE;

  /// Proxy peloton::executor::ExecutorContext::GetTransaction()
  DECLARE_METHOD(GetTransaction);
};

TYPE_BUILDER(ExecutorContext, executor::ExecutorContext);

}  // namespace codegen
}  // namespace peloton
