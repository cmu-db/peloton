//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_builder.h
//
// Identification: src/include/codegen/proxy/type_builder.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {
namespace proxy {

/// This class contains LLVM llvm::Type* builders for the most common C++ types
/// that peloton uses. The basic primitive types are covered, including arrays
/// references, pointers, const-anything, functions, function pointers and
/// member function pointers.
///
/// If you define a proxy type (i.e., a custom data type that you wish to access
/// from codegen), you ***MUST*** provide an implementation of TypeBuilder for
/// your type (i.e., TypeBuilder<YourType>). You can use the handy
/// TYPE_BUILDER(...) macro for your type that delegates to the proxy class you
/// should have defined in include/codegen/proxy/proxy.h.

template <typename T>
struct TypeBuilder {};

#define DEFINE_PRIMITIVE_BUILDER(P, N)                           \
  template <>                                                    \
  struct TypeBuilder<P> {                                        \
    static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE { \
      return codegen.N##Type();                                  \
    }                                                            \
  };
DEFINE_PRIMITIVE_BUILDER(void, Void);
DEFINE_PRIMITIVE_BUILDER(bool, Bool);
DEFINE_PRIMITIVE_BUILDER(char, Int8);
DEFINE_PRIMITIVE_BUILDER(signed char, Int8);
DEFINE_PRIMITIVE_BUILDER(unsigned char, Int8);
DEFINE_PRIMITIVE_BUILDER(short, Int16);
DEFINE_PRIMITIVE_BUILDER(unsigned short, Int16);
DEFINE_PRIMITIVE_BUILDER(int, Int32);
DEFINE_PRIMITIVE_BUILDER(unsigned int, Int32);
DEFINE_PRIMITIVE_BUILDER(long, Int64);
DEFINE_PRIMITIVE_BUILDER(unsigned long, Int64);
DEFINE_PRIMITIVE_BUILDER(long long, Int64);
DEFINE_PRIMITIVE_BUILDER(unsigned long long, Int64);
DEFINE_PRIMITIVE_BUILDER(double, Double);
DEFINE_PRIMITIVE_BUILDER(void *, VoidPtr);
DEFINE_PRIMITIVE_BUILDER(char *, CharPtr);
DEFINE_PRIMITIVE_BUILDER(unsigned char *, CharPtr);
#undef DEFINE_PRIMITIVE_BUILDER

/// Const
template <typename T>
struct TypeBuilder<const T> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return TypeBuilder<T>::GetType(codegen);
  }
};

/// Pointers and references
template <typename T>
struct TypeBuilder<T *> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return TypeBuilder<T>::GetType(codegen)->getPointerTo();
  }
};
template <typename T>
struct TypeBuilder<T &> : public TypeBuilder<T *> {};

/// Fixed-length arrays
template <typename T, uint32_t size>
struct TypeBuilder<T[size]> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return llvm::ArrayType::get(TypeBuilder<T>::GetType(codegen), size);
  }
};

/// Regular C-style functions
template <typename Ret, typename... Args>
struct TypeBuilder<Ret(Args...)> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    llvm::Type *ret_type = TypeBuilder<Ret>::GetType(codegen);
    std::vector<llvm::Type *> arg_types = {
        TypeBuilder<Args>::GetType(codegen)...};
    return llvm::FunctionType::get(ret_type, arg_types, false);
  }
};

/// C-style function pointer
template <typename Ret, typename... Args>
struct TypeBuilder<Ret (*)(Args...)> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    llvm::Type *ret_type = TypeBuilder<Ret>::GetType(codegen);
    std::vector<llvm::Type *> arg_types = {
        TypeBuilder<Args>::GetType(codegen)...};
    return llvm::FunctionType::get(ret_type, arg_types, false)->getPointerTo();
  }
};

/// Regular C-style functions with variable arguments
template <typename Ret, typename... Args>
struct TypeBuilder<Ret(Args..., ...)> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    llvm::Type *ret_type = TypeBuilder<Ret>::GetType(codegen);
    std::vector<llvm::Type *> arg_types = {
        TypeBuilder<Args>::GetType(codegen)...};
    return llvm::FunctionType::get(ret_type, arg_types, true);
  }
};

/// C-style function pointer with variable arguments
template <typename Ret, typename... Args>
struct TypeBuilder<Ret (*)(Args..., ...)> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    llvm::Type *ret_type = TypeBuilder<Ret>::GetType(codegen);
    std::vector<llvm::Type *> arg_types = {
        TypeBuilder<Args>::GetType(codegen)...};
    return llvm::FunctionType::get(ret_type, arg_types, true)->getPointerTo();
  }
};

/// Member functions
template <typename R, typename T, typename... Args>
struct TypeBuilder<R (T::*)(Args...)> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    std::vector<llvm::Type *> arg_types = {TypeBuilder<T *>::GetType(codegen)};
    arg_types.insert(arg_types.end(), {TypeBuilder<Args>::GetType(codegen)...});
    return llvm::FunctionType::get(TypeBuilder<R>::GetType(codegen), arg_types,
                                   false)->getPointerTo();
  }
};
template <typename R, typename T, typename... Args>
struct TypeBuilder<R (T::*)(Args...) const>
    : public TypeBuilder<R (T::*)(Args...)> {};

}  // namespace proxy
}  // namespace codegen
}  // namespace peloton