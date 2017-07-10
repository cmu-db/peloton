//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter_proxy.h
//
// Identification: src/include/codegen/proxy/deleter_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/transaction_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// DELETER PROXY
//===----------------------------------------------------------------------===//

struct DeleterProxy {
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kDeleterTypeName = "peloton::codegen::Deleter";
    auto *deleter_type = codegen.LookupTypeByName(kDeleterTypeName);
    if (deleter_type != nullptr) {
      return deleter_type;
    }

    // Type isn't cached, create it
    auto *opaque_arr_type =
        codegen.VectorType(codegen.Int8Type(), sizeof(Deleter));
    return llvm::StructType::create(codegen.GetContext(), {opaque_arr_type},
                                    kDeleterTypeName);
  }

  // Wrapper around Delete::Init()
  struct _Init {
    static const std::string &GetFunctionName() {
      static const std::string init_fn_name =
#ifdef __APPLE__
          "_ZN7peloton7codegen7Deleter4InitEPNS_11concurrency11TransactionEPNS_"
          "7storage9DataTableE";
#else
          "_ZN7peloton7codegen7Deleter4InitEPNS_11concurrency11TransactionEPNS_"
          "7storage9DataTableE";
#endif
      return init_fn_name;
    }

    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      // Has the function already been registered?
      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          DeleterProxy::GetType(codegen)->getPointerTo(),
          TransactionProxy::GetType(codegen)->getPointerTo(),
          DataTableProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };

  // Wrapper around Deleter::Delete()
  struct _Delete {
    static const std::string &GetFunctionName() {
      static const std::string delete_fn_name =
#ifdef __APPLE__
          "_ZN7peloton7codegen7Deleter6DeleteEjj";
#else
          "_ZN7peloton7codegen7Deleter6DeleteEjj";
#endif
      return delete_fn_name;
    }

    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      // Has the function already been registered?
      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          DeleterProxy::GetType(codegen)->getPointerTo(), codegen.Int32Type(),
          codegen.Int32Type()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };
};

}  // namespace codegen
}  // namespace peloton