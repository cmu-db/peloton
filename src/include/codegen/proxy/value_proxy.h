//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.h
//
// Identification: src/include/codegen/proxy/value_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

PROXY(Value) {
  DECLARE_MEMBER(0, char[sizeof(peloton::type::Value)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Value, peloton::type::Value);

}  // namespace codegen
}  // namespace peloton