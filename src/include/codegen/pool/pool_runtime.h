//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_runtime.h
//
// Identification: src/include/codegen/pool/pool_runtime.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

class PoolRuntime {
 public:
  static type::AbstractPool *CreatePool();

  static void DeletePool(type::AbstractPool *pool);
};

}  // namespace codegen
}  // namespace peloton
