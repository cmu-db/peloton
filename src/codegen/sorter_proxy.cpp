//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.cpp
//
// Identification: src/codegen/sorter_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/codegen/proxy/sorter_proxy.h"
#include "codegen/util/sorter.h"

namespace peloton {
namespace codegen {

llvm::Type *SorterProxy::GetType(CodeGen &codegen) {
  static const std::string kSorterTypeName = "peloton::codegen::util::Sorter";

  auto *sorter_type = codegen.LookupTypeByName(kSorterTypeName);
  if (sorter_type != nullptr) {
    return sorter_type;
  }

  // Do a compile-time assertion to make sure what we build here and what
  // the actual layout of the Sorter class is actually match
  /*
  static const uint32_t sorter_size =
      sizeof(char *) + sizeof(char *) + sizeof(char *) + sizeof(uint32_t) +
      sizeof(util::Sorter::ComparisonFunction) +
      sizeof(storage::StorageManager *);
  static_assert(
      sorter_size == sizeof(util::Sorter),
      "The LLVM memory layout of Sorter doesn't match the pre-compiled "
      "version. Did you forget to update codegen/sorter_proxy.h?");
  */

  // Ensure function pointers work
  static_assert(
      sizeof(codegen::util::Sorter::ComparisonFunction) == sizeof(char *),
      "Function pointer size is messed up.");

  // Sorter type doesn't exist in module, construct it now
  std::vector<llvm::Type *> sorter_fields = {
      codegen.CharPtrType(),  // buffer start
      codegen.CharPtrType(),  // buffer position
      codegen.CharPtrType(),  // buffer end
      codegen.Int32Type(),    // tuple size
      codegen.CharPtrType()   // comparison function pointer
  };
  sorter_type = llvm::StructType::create(codegen.GetContext(), sorter_fields,
                                         kSorterTypeName);
  return sorter_type;
}

//===--------------------------------------------------------------------===//
// The proxy for codegen::util::Sorter::Init()
//===--------------------------------------------------------------------===//
const std::string &SorterProxy::_Init::GetFunctionName() {
  static const std::string kInitFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen5utils6Sorter4InitEPFiPKvS4_Ej";
#else
      "_ZN7peloton7codegen5utils6Sorter4InitEPFiPKvS4_Ej";
#endif
  return kInitFnName;
}

llvm::Function *SorterProxy::_Init::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now ...

  // The function type of the comparison function is int(*)(void *, void*)
  auto *comparison_fn_type = llvm::FunctionType::get(
      codegen.Int32Type(), {codegen.CharPtrType(), codegen.CharPtrType()},
      false);

  // We need to create a function type whose signature matches
  // codegen::util::Sorter::Init(...). It should match:
  //
  // void Init(Sorter *, int(*)(void *, void *), uint32_t)

  std::vector<llvm::Type *> fn_args = {
      SorterProxy::GetType(codegen)->getPointerTo(),
      comparison_fn_type->getPointerTo(), codegen.Int32Type()};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===--------------------------------------------------------------------===//
// The proxy for codegen::util::Sorter::StoreInputTuple()
//===--------------------------------------------------------------------===//
const std::string &SorterProxy::_StoreInputTuple::GetFunctionName() {
  static const std::string kStoreInputTupleFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen5utils6Sorter15StoreInputTupleEv";
#else
      "_ZN7peloton7codegen5utils6Sorter15StoreInputTupleEv";
#endif
  return kStoreInputTupleFnName;
}

llvm::Function *SorterProxy::_StoreInputTuple::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now ...
  // We need to create a function type whose signature matches
  // codegen::util::Sorter::StoreInputTuple(...)
  std::vector<llvm::Type *> fn_args = {
      SorterProxy::GetType(codegen)->getPointerTo()};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.CharPtrType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===--------------------------------------------------------------------===//
// The proxy for codegen::util::Sorter::Sort()
//===--------------------------------------------------------------------===//
const std::string &SorterProxy::_Sort::GetFunctionName() {
  static const std::string kSortFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen5utils6Sorter4SortEv";
#else
      "_ZN7peloton7codegen5utils6Sorter4SortEv";
#endif
  return kSortFnName;
}

llvm::Function *SorterProxy::_Sort::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now ...
  // We need to create a function type whose signature matches
  // codegen::util::Sorter::Sort(...)
  std::vector<llvm::Type *> fn_args = {
      SorterProxy::GetType(codegen)->getPointerTo()};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

//===--------------------------------------------------------------------===//
// The proxy for codegen::util::Sorter::Destroy()
//===--------------------------------------------------------------------===//
const std::string &SorterProxy::_Destroy::GetFunctionName() {
  static const std::string kDestroyFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen5utils6Sorter7DestroyEv";
#else
      "_ZN7peloton7codegen5utils6Sorter7DestroyEv";
#endif
  return kDestroyFnName;
}

llvm::Function *SorterProxy::_Destroy::GetFunction(CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now ...
  // We need to create a function type whose signature matches
  // codegen::util::Sorter::Destroy(...)
  std::vector<llvm::Type *> fn_args = {
      SorterProxy::GetType(codegen)->getPointerTo()};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
