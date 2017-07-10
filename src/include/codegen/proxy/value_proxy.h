//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.h
//
// Identification: src/include/codegen/proxy/value_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

PROXY(Value) {
  PROXY_MEMBER_FIELD(0, char[sizeof(peloton::type::Value)], opaque);
  PROXY_TYPE("peloton::Value", char[sizeof(peloton::type::Value)]);
};

namespace proxy {
template <>
struct TypeBuilder<peloton::type::Value> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return ValueProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
