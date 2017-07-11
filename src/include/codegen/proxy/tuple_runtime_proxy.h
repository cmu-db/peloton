//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/tuple_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/type/type.h"
#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

class TupleRuntimeProxy {
 public:
  struct _CreateVarArea {
    static const std::string &GetFunctionName() {
      static const std::string kCreateVarAreaFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen12TupleRuntime13CreateVarAreaEPcjS2_PNS_4type"
          "12AbstractPoolE";
#else
          "_ZN7peloton7codegen12TupleRuntime13CreateVarAreaEPcjS2_PNS_4type"
          "12AbstractPoolE";
#endif
      return kCreateVarAreaFnName;
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
