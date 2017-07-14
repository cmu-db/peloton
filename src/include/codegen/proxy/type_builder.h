//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_builder.h
//
// Identification: src/include/codegen/proxy/type_builder.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"

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
struct TypeBuilder<ProxyMember<Pos, T>> : public TypeBuilder<T> {};

}  // namespace proxy
}  // namespace codegen
}  // namespace peloton