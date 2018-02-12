//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_proxy.h
//
// Identification: src/include/codegen/proxy/pool_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

PROXY(AbstractPool) {
  DECLARE_MEMBER(0, char[sizeof(peloton::type::AbstractPool)], opaque);
  DECLARE_TYPE;
};

PROXY(EphemeralPool) {
  DECLARE_MEMBER(0, char[sizeof(peloton::type::EphemeralPool)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(AbstractPool, peloton::type::AbstractPool);
TYPE_BUILDER(EphemeralPool, peloton::type::EphemeralPool);

}  // namespace codegen
}  // namespace peloton
