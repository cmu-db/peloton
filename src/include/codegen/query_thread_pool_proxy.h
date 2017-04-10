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

  //===--------------------------------------------------------------------===//
  // LLVM wrapper/definition for QueryThreadPool::SubmitQueryTask(...) to
  // submit a task to the thread pool
  //===--------------------------------------------------------------------===//
  struct _SubmitQueryTask {
    // Get the symbol name
    static const std::string &GetFunctionName();
    // Get the actual function definition
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
