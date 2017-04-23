//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_tuple_runtime_proxy.cpp
//
// Identification: src/codegen/raw_tuple/raw_tuple_runtime_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pool/pool_runtime_proxy.h"
#include "codegen/transaction_proxy.h"
#include "codegen/data_table_proxy.h"
#include "codegen/insert/insert_helpers_proxy.h"
#include "codegen/raw_tuple/raw_tuple_runtime_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

const std::string &RawTupleRuntimeProxy::_SetVarLen::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen15RawTupleRuntime9SetVarLenEjPcS2_PNS_4type12AbstractPoolE";
  return kInsertValueFnName;
}

llvm::Function *RawTupleRuntimeProxy::_SetVarLen::GetFunction(
    CodeGen &codegen) {

  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          codegen.Int32Type(),                // len
          codegen.Int8Type()->getPointerTo(), // data
          codegen.Int8Type()->getPointerTo(), // buf
          PoolRuntimeProxy::GetType(codegen)->getPointerTo(), // pool
      },
      false
  );

  return codegen.RegisterFunction(fn_name, fn_type);

}

const std::string &RawTupleRuntimeProxy::_DumpTuple::GetFunctionName() {
  static const std::string kInsertValueFnName =
      "_ZN7peloton7codegen15RawTupleRuntime9DumpTupleEPNS_7storage5TupleE";
  return kInsertValueFnName;
}

llvm::Function *RawTupleRuntimeProxy::_DumpTuple::GetFunction(
    CodeGen &codegen) {

  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      { codegen.Int8Type()->getPointerTo() },
      false);

  return codegen.RegisterFunction(fn_name, fn_type);

}


}  // namespace codegen
}  // namespace peloton
