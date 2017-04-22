//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_runtime_proxy.h
//
// Identification: src/include/codegen/pool/pool_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"
#include "codegen/pool/pool_runtime.h"
#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class PoolRuntimeProxy {
 public:
  static llvm::Type* GetType(CodeGen &codegen);

  struct _CreatePool {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _DeletePool {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
