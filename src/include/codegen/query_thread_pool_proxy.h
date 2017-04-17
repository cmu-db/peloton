//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool_proxy.h
//
// Identification: src/include/codegen/thread_pool_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class QueryThreadPoolProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen);

  static llvm::Function *GetSubmitQueryTaskFunction(CodeGen &codegen);
  static llvm::Function *GetGetIntanceFunction(CodeGen &codegen);
};

}  // namespace codegen
}  // namespace peloton
