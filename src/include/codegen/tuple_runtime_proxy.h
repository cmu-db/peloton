//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime_proxy.h
//
// Identification: src/include/codegen/tuple_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/type.h"
#include "codegen/pool_proxy.h"

namespace peloton {
namespace codegen {

class TupleRuntimeProxy {
 public:
  struct _MaterializeVarLen {
    static const std::string &GetFunctionName() {
      static const std::string kMaterializeVarLenFnName =
          "_ZN7peloton7codegen12TupleRuntime17MaterializeVarLenEPcjS2_PNS_4type"
          "12AbstractPoolE";
      return kMaterializeVarLenFnName;
    }

    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      // Has the function already been registered?
      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }
    
      auto *fn_type = llvm::FunctionType::get(
          codegen.VoidType(),
          {codegen.Int8Type()->getPointerTo(), codegen.Int32Type(), 
           codegen.Int8Type()->getPointerTo(),
           PoolProxy::GetType(codegen)->getPointerTo()},
          false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };
};

}  // namespace codegen
}  // namespace peloton
