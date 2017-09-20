//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.cpp
//
// Identification: src/codegen/proxy/value_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/value_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Value, "peloton::Value", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
