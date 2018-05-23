//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_proxy.cpp
//
// Identification: src/codegen/proxy/pool_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(AbstractPool, "type::AbstractPool", opaque);
DEFINE_TYPE(EphemeralPool, "type::EphemeralPool", opaque);

}  // namespace codegen
}  // namespace peloton
