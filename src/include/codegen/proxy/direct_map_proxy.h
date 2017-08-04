//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// direct_map_proxy.h
//
// Identification: src/include/codegen/proxy/direct_map_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "type/types.h"

namespace peloton {
namespace codegen {

PROXY(DirectMap) {
  DECLARE_MEMBER(0, char[sizeof(peloton::DirectMap)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(DirectMap, peloton::DirectMap);

}  // namespace codegen
}  // namespace peloton
