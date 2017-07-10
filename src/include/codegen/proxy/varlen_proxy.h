//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_proxy.h
//
// Identification: src/include/codegen/proxy/varlen_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "type/varlen_type.h"

namespace peloton {
namespace codegen {

PROXY(Varlen) {
  PROXY_MEMBER_FIELD(0, uint32_t, length);
  PROXY_MEMBER_FIELD(0, const char, ptr);
  PROXY_TYPE("peloton::Varlen", uint32_t, const char);
};

}  // namespace codegen
}  // namespace peloton