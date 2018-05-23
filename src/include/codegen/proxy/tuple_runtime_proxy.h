//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/tuple_runtime_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(TupleRuntime) {
  DECLARE_METHOD(CreateVarlen);
};

}  // namespace codegen
}  // namespace peloton
