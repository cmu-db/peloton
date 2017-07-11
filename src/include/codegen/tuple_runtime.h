//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime.h
//
// Identification: src/include/codegen/tuple_runtime.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"

namespace peloton {

namespace type {
class AbstractPool;
}  // namespace type

namespace codegen {

class TupleRuntime {
 public:
  static void CreateVarArea(char *data, uint32_t len, char *buf,
                            peloton::type::AbstractPool *pool);
};

}  // namespace codegen
}  // namespace peloton
