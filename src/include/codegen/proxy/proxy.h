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

#include <type_traits>

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {
namespace proxy {

template <typename T>
struct TypeBuilder {};

/// void type
template <>
struct TypeBuilder<void> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.VoidType();
  }
};

/// Primitive types

/// bool
template <>
struct TypeBuilder<bool> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.BoolType();
  }
};

/// int8
template <>
struct TypeBuilder<int8_t> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.Int8Type();
  }
};
template <>
struct TypeBuilder<uint8_t> : public TypeBuilder<int8_t> {};
template <>
struct TypeBuilder<char> : public TypeBuilder<int8_t> {};

/// int16
template <>
struct TypeBuilder<int16_t> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.Int16Type();
  }
};
template <>
struct TypeBuilder<uint16_t> : public TypeBuilder<int16_t> {};

/// int32
template <>
struct TypeBuilder<int32_t> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.Int32Type();
  }
};
template <>
struct TypeBuilder<uint32_t> : public TypeBuilder<int32_t> {};

/// int64
template <>
struct TypeBuilder<int64_t> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.Int64Type();
  }
};
template <>
struct TypeBuilder<uint64_t> : public TypeBuilder<int64_t> {};

/// double
template <>
struct TypeBuilder<double> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return codegen.DoubleType();
  }
};

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

/// A list of types
template <typename... Args>
struct TypeList {
  static std::vector<llvm::Type *> GetType(CodeGen &codegen) ALWAYS_INLINE {
    return {TypeBuilder<Args>::GetType(codegen)...};
  }
};

template <uint32_t Pos, typename T>
struct Member {
  using Type = T;
};

#define PROXY(N) struct N##Proxy

#define PROXY_MEMBER_FIELD(P, T, N) peloton::codegen::proxy::Member<P, T> _##N

#define FIELDS(...) \
  (peloton::codegen::proxy::TypeList<__VA_ARGS__>::GetType(codegen))

#define PROXY_TYPE(N, ...)                                                    \
  static llvm::Type *GetType(peloton::codegen::CodeGen &codegen) {            \
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

#define PROXY_METHOD(M, PTR, N)                                               \
  struct _##M {                                                               \
    /*static constexpr const char *k##M##FnName = "__FILL__ME__";*/           \
    static constexpr const char *k##M##FnName = N;                            \
    static llvm::Function *GetFunction(peloton::codegen::CodeGen &codegen) {  \
      /* Check if the function has already been defined - return it if so */  \
      llvm::Function *func = codegen.LookupFunction(k##M##FnName);            \
      if (func != nullptr) {                                                  \
        return func;                                                          \
      }                                                                       \
                                                                              \
      /* Ensure the TypeBuilder for the given method actually produces an     \
       * LLVM FunctionType */                                                 \
      static_assert(                                                          \
          ((std::is_pointer<decltype(PTR)>::value &&                          \
            std::is_function<                                                 \
                typename std::remove_pointer<decltype(PTR)>::type>::value) || \
           std::is_member_function_pointer<decltype(PTR)>::value),            \
          "You must provide a pointer to the function you want to proxy");    \
                                                                              \
      /* The function hasn't been registered. Do it now */                    \
      llvm::PointerType *func_ptr = llvm::cast<llvm::PointerType>(            \
          peloton::codegen::proxy::TypeBuilder<decltype(PTR)>::GetType(       \
              codegen));                                                      \
      llvm::FunctionType *func_type =                                         \
          llvm::cast<llvm::FunctionType>(func_ptr->getElementType());         \
      return codegen.RegisterFunction(k##M##FnName, func_type);               \
    }                                                                         \
  };

}  // namespace proxy
}  // namespace codegen
}  // namespace peloton