//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_tuple_runtime_proxy.h
//
// Identification: src/include/codegen/raw_tuple/raw_tuple_runtime_proxy.h
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

namespace peloton {
namespace codegen {

class RawTupleRuntimeProxy {
 public:
  struct _SetVarLen {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _DumpTuple {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
