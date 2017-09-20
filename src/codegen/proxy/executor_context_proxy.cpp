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

namespace peloton {
namespace codegen {

DEFINE_TYPE(ExecutorContext, "executor::ExecutorContext", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
