//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_tuple_runtime.cpp
//
// Identification: src/codegen/raw_tuple/raw_tuple_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/raw_tuple/raw_tuple_runtime.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {

void RawTupleRuntime::SetVarLen(uint32_t len, char *data,
                                char *buf, type::AbstractPool *pool) {
  (void)pool;

  struct varlen_t {
    uint32_t len;
    char data[];
  };

//  varlen_t *actual = reinterpret_cast<varlen_t *>(
//      pool->Allocate(len + sizeof(uint32_t))
//  );

  varlen_t *actual = reinterpret_cast<varlen_t *>(
      malloc(len + sizeof(uint32_t))
  );

  actual->len = len;
  PL_MEMCPY(actual->data, data, len);

  *reinterpret_cast<varlen_t **>(buf) = actual;
}

void RawTupleRuntime::DumpTuple(storage::Tuple *tuple) {
  const catalog::Schema *schema = tuple->GetSchema();
  for (oid_t i = 0; i != schema->GetColumnCount(); ++i) {
    type::Value value = tuple->GetValue(i);
    std::cout << value << "\t";
  }
  std::cout << std::endl;
}

}  // namespace codegen
}  // namespace peloton
