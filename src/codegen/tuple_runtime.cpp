//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime.cpp
//
// Identification: src/codegen/tuple_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tuple_runtime.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {

void TupleRuntime::CreateVarlen(char *data, uint32_t len, char *buf,
                                peloton::type::AbstractPool *pool) {
  struct varlen_t {
    uint32_t len;
    char data[0];
  };

  auto *area =
      reinterpret_cast<varlen_t *>(pool->Allocate(sizeof(uint32_t) + len));
  area->len = len;
  PL_MEMCPY(area->data, data, len);

  *reinterpret_cast<varlen_t **>(buf) = area;
}

}  // namespace codegen
}  // namespace peloton
