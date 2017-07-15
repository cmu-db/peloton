//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter_proxy.h
//
// Identification: src/include/codegen/proxy/inserter_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/inserter.h"
#include "codegen/proxy/tuple_proxy.h"
#include "codegen/proxy/pool_proxy.h"
#include "codegen/proxy/transaction_proxy.h"

namespace peloton {
namespace codegen {

class InserterProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kInserterTypeName = "peloton::codegen::Inserter";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto inserter_type = codegen.LookupTypeByName(kInserterTypeName);
    if (inserter_type != nullptr) {
      return inserter_type;
    }
  
    // Type isn't cached, create a new one
    auto *opaque_arr_type =
        codegen.VectorType(codegen.Int8Type(), sizeof(Inserter));
    return llvm::StructType::create(codegen.GetContext(), {opaque_arr_type},
                                  kInserterTypeName);
  }

  struct _Init {
    static const std::string &GetFunctionName() {
      static const std::string kInitFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen8Inserter4InitEPNS_11concurrency11TransactionEPNS"
          "_7storage9DataTableEPNS_8executor15ExecutorContextE";
#else
          "_ZN7peloton7codegen8Inserter4InitEPNS_11concurrency11TransactionEPNS"
          "_7storage9DataTableEPNS_8executor15ExecutorContextE";
#endif
      return kInitFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          InserterProxy::GetType(codegen)->getPointerTo(),
          TransactionProxy::GetType(codegen)->getPointerTo(),
          DataTableProxy::GetType(codegen)->getPointerTo(),
          ExecutorContextProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };

  struct _ReserveTupleStorage {
    static const std::string &GetFunctionName() {
      static const std::string kReserveTupleStorageFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen8Inserter19ReserveTupleStorageEv";
#else
          "_ZN7peloton7codegen8Inserter19ReserveTupleStorageEv";
#endif
      return kReserveTupleStorageFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          InserterProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type = llvm::FunctionType::get(
          codegen.CharPtrType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };

  struct _GetPool {
    static const std::string &GetFunctionName() {
      static const std::string kGetPoolFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen8Inserter7GetPoolEv";
#else
          "_ZN7peloton7codegen8Inserter7GetPoolEv";
#endif
      return kGetPoolFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          InserterProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type = llvm::FunctionType::get(
          PoolProxy::GetType(codegen)->getPointerTo(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };


  struct _InsertReserved {
    static const std::string &GetFunctionName() {
      static const std::string kInsertReservedFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen8Inserter14InsertReservedEv";
#else
          "_ZN7peloton7codegen8Inserter14InsertReservedEv";
#endif
      return kInsertReservedFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          InserterProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };

  struct _Insert {
    static const std::string &GetFunctionName() {
      static const std::string kInsertFnName =
#ifdef __APPLE__
          "_ZN7peloton7codegen8Inserter6InsertEPKNS_7storage5TupleE";
#else
          "_ZN7peloton7codegen8Inserter6InsertEPKNS_7storage5TupleE";
#endif
      return kInsertFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          InserterProxy::GetType(codegen)->getPointerTo(),
          TupleProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };
};

}  // namespace codegen
}  // namespace peloton
