//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime_proxy.cpp
//
// Identification: src/codegen/proxy/tuple_runtime_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/tuple_runtime_proxy.h"

#include "codegen/tuple_runtime.h"
#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen, TupleRuntime, CreateVarlen);

}  // namespace codegen
}  // namespace peloton
