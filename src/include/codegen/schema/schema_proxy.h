//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema_proxy.h
//
// Identification: src/include/codegen/schema_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class SchemaProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen);
};

}  // namespace codegen
}  // namespace peloton
