//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/transaction_runtime_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
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

  // The proxy around TransactionRuntime::PerformDelete()
  struct _PerformDelete {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around TransactionRuntime::IncreaseNumProcessed()
  struct _IncreaseNumProcessed {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namepsace codegen
}  // namespace peloton
