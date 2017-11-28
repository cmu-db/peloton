//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_proxy.cpp
//
// Identification: src/codegen/proxy/index_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/index_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Index, "index::Index", MEMBER(opaque));
// DEFINE_METHOD(peloton::index, Index, CodeGenScan);

}  // namespace codegen
}  // namespace peloton
