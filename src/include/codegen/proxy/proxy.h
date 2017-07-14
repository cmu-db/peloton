//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// proxy.h
//
// Identification: src/include/codegen/proxy/proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

/// This file contains several macros that help in creating proxies to C++
/// classes.

#define PROXY(N) struct N##Proxy

#define DECLARE_MEMBER(P, T, N) \
  static const peloton::codegen::ProxyMember<P, T> _##N

#define DECLARE_TYPE \
  static llvm::Type *GetType(peloton::codegen::CodeGen &codegen);

#define DECLARE_METHOD(N)                                                  \
  struct _##N : public ProxyMethod<_##N> {                                 \
    static const char *k##N##FnName;                                       \
    llvm::Function *GetFunction(peloton::codegen::CodeGen &codegen) const; \
  };                                                                       \
  static _##N N;

#define MEMBER(N) decltype(_##N)
#define FIELDS(...) \
  (peloton::codegen::proxy::TypeList<__VA_ARGS__>::GetType(codegen))

#define DEFINE_TYPE(P, N, ...)                                                \
  llvm::Type *P##Proxy::GetType(peloton::codegen::CodeGen &codegen) {         \
    static constexpr const char *kTypeName = N;                               \
    /* Check if type has already been registered */                           \
    llvm::Type *type = codegen.LookupTypeByName(kTypeName);                   \
    if (type != nullptr) {                                                    \
      return type;                                                            \
    }                                                                         \
    std::vector<llvm::Type *> fields = (FIELDS(__VA_ARGS__));                 \
    type = llvm::StructType::create(codegen.GetContext(), fields, kTypeName); \
    return type;                                                              \
  }

// P: Proxy, N: method name, PTR: C++ method pointer, MNAME: mangled name
#define DEFINE_METHOD(P, N, PTR, MNAME)                                      \
  P##Proxy::_##N P##Proxy::N = {};                                           \
  const char *P##Proxy::_##N::k##N##FnName = MNAME;                          \
  llvm::Function *P##Proxy::_##N::GetFunction(                               \
      peloton::codegen::CodeGen &codegen) const {                            \
    /* Check if the function has already been defined - return it if so */   \
    llvm::Function *func = codegen.LookupFunction(k##N##FnName);             \
    if (func != nullptr) {                                                   \
      return func;                                                           \
    }                                                                        \
                                                                             \
    /* Ensure the provided function is either a function pointer or a member \
     * function pointer                                                      \
     * member function pointer */                                            \
    static_assert(                                                           \
        ((std::is_pointer<decltype(PTR)>::value &&                           \
          std::is_function<                                                  \
              typename std::remove_pointer<decltype(PTR)>::type>::value) ||  \
         std::is_member_function_pointer<decltype(PTR)>::value),             \
        "You must provide a pointer to the function you want to proxy");     \
                                                                             \
    /* The function hasn't been registered. Do it now */                     \
    llvm::PointerType *func_ptr = llvm::cast<llvm::PointerType>(             \
        peloton::codegen::proxy::TypeBuilder<decltype(PTR)>::GetType(        \
            codegen));                                                       \
    llvm::FunctionType *func_type =                                          \
        llvm::cast<llvm::FunctionType>(func_ptr->getElementType());          \
    return codegen.RegisterFunction(k##N##FnName, func_type);                \
  }

#define TYPE_BUILDER(PROXY, TYPE)                                  \
  namespace proxy {                                                \
  template <>                                                      \
  struct TypeBuilder<TYPE> {                                       \
    static llvm::Type *GetType(peloton::codegen::CodeGen &codegen) \
        ALWAYS_INLINE {                                            \
      return PROXY##Proxy::GetType(codegen);                       \
    }                                                              \
  };                                                               \
  }

}  // namespace codegen
}  // namespace peloton