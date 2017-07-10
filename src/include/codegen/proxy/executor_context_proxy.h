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

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {

PROXY(ExecutorContext) {
  PROXY_MEMBER_FIELD(0, char[sizeof(executor::ExecutorContext)], opaque);
  PROXY_TYPE("peloton::executor::ExecutorContext",
             char[sizeof(executor::ExecutorContext)]);
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::executor::ExecutorContext> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return ExecutorContextProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
