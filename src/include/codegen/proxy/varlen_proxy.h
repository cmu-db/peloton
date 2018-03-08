//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_proxy.h
//
// Identification: src/include/codegen/proxy/varlen_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "type/varlen_type.h"

namespace peloton {
namespace codegen {

PROXY(Varlen) {
  DECLARE_MEMBER(0, uint32_t, length);
  DECLARE_MEMBER(1, const char, ptr);
  DECLARE_TYPE;
};

}  // namespace codegen
}  // namespace peloton