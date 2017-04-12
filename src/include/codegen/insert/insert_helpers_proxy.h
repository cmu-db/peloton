//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.h
//
// Identification: src/include/codegen/insert/insert_helpers_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value_peeker.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class InsertHelpersProxy {
 public:
  struct _InsertValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};


}  // namespace codegen
}  // namespace peloton
