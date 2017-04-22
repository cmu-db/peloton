//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_tuple_runtime.h
//
// Identification: src/include/codegen/raw_tuple/raw_tuple_runtime.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/schema.h"
#include "planner/attribute_info.h"
#include "planner/binding_context.h"
#include "codegen/row_batch.h"
#include "codegen/type.h"
#include "type/types.h"
#include "storage/tuple.h"

namespace peloton {
namespace codegen {

class RawTupleRuntime {
 public:
  static void SetVarLen(uint32_t len, char *data,
                        char *buf, type::AbstractPool *pool);

  static void DumpTuple(storage::Tuple *tuple);
};

}  // namespace codegen
}  // namespace peloton
