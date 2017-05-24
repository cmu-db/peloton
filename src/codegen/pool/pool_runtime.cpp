//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_runtime.cpp
//
// Identification: src/codegen/pool/pool_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pool/pool_runtime.h"
#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

type::AbstractPool *PoolRuntime::CreatePool() {
  type::AbstractPool *pool = new type::EphemeralPool();
  return pool;
}

void PoolRuntime::DeletePool(type::AbstractPool *pool) { delete pool; }

}  // namespace codegen
}  // namespace peloton
