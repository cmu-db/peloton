//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// direct_map_proxy.h
//
// Identification: src/codegen/proxy/direct_map_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/direct_map_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(DirectMap, "peloton::DirectMap", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
