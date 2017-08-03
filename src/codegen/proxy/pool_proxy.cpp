//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_proxy.cpp
//
// Identification: src/codegen/proxy/pool_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(AbstractPool, "type::AbstractPool", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
