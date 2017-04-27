//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.h
//
// Identification: src/include/codegen/transaction_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class TransactionRuntimeProxy {
 public:
  // The proxy around TransactionRuntime::PerformVectorizedRead()
  struct _PerformVectorizedRead {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namepsace codegen
}  // namespace peloton