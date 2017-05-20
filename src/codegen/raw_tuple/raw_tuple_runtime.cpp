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
#include <sstream>
#include "common/logger.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {

void RawTupleRuntime::SetVarLen(uint32_t len, char *data, char *buf,
                                type::AbstractPool *pool) {
  (void)pool;

  struct varlen_t {
    uint32_t len;
    char data[];
  };

  varlen_t *actual =
      reinterpret_cast<varlen_t *>(pool->Allocate(len + sizeof(uint32_t)));

  actual->len = len;
  PL_MEMCPY(actual->data, data, len);

  *reinterpret_cast<varlen_t **>(buf) = actual;
}

void RawTupleRuntime::DumpTuple(storage::Tuple *tuple) {
  std::ostringstream os;
  const catalog::Schema *schema = tuple->GetSchema();
  for (oid_t i = 0; i != schema->GetColumnCount(); ++i) {
    type::Value value = tuple->GetValue(i);
    os << value << "\t";
  }
  LOG_DEBUG("%s", os.str().c_str());
}

}  // namespace codegen
}  // namespace peloton
