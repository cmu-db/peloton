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
  static const ::peloton::codegen::ProxyMember<P, T> _##N;

#define DECLARE_TYPE \
  static ::llvm::Type *GetType(::peloton::codegen::CodeGen &codegen);

#define DECLARE_METHOD(N)                                                      \
  struct _##N : public ::peloton::codegen::ProxyMethod<_##N> {                 \
    static const char *k##N##FnName;                                           \
    ::llvm::Function *GetFunction(::peloton::codegen::CodeGen &codegen) const; \
  };                                                                           \
  static _##N N;

#define MEMBER(N) decltype(_##N)
#define FIELDS(...) \
  (::peloton::codegen::proxy::TypeList<__VA_ARGS__>::GetType(codegen))

#define DEFINE_TYPE(P, N, ...)                                            \
  ::llvm::Type *P##Proxy::GetType(::peloton::codegen::CodeGen &codegen) { \
    static constexpr const char *kTypeName = N;                           \
    /* Check if type has already been registered */                       \
    ::llvm::Type *type = codegen.LookupType(kTypeName);                   \
    if (type != nullptr) {                                                \
      return type;                                                        \
    }                                                                     \
    ::std::vector<::llvm::Type *> fields = (FIELDS(__VA_ARGS__));         \
    return ::llvm::StructType::create(codegen.GetContext(), fields,       \
                                      kTypeName);                         \
  }

namespace proxy {
namespace detail {

//===----------------------------------------------------------------------===//
// The Itanium ABI does not provide the location of virtual functions. Thus, all
// virtual method invocations go through a VirtualDispatch thunk that does the
// heavy lifting of adjusting *this and calculating vtable offsets to invoke the
// correct function. Since these functions are static, their address is known at
// runtime, and is used as the entry point for the virtual function.
//
// You had better not be calling virtual functions from hot-loops since LLVM
// cannot inline or optimize those calls because the function definition is not
// available.
//===----------------------------------------------------------------------===//

template <typename X, typename T, T t>
struct VirtualDispatch;

template <typename R, typename C, typename... Args, typename T, T MemFn>
struct VirtualDispatch<R (C::*)(Args...), T, MemFn> {
  static R Fn(C *obj, Args... args) { return (obj->*MemFn)(args...); }
};

template <typename R, typename C, typename... Args, typename T, T MemFn>
struct VirtualDispatch<R (C::*)(Args...) const, T, MemFn> {
  static R Fn(C *obj, Args... args) { return (obj->*MemFn)(args...); }
};

template <typename C, typename... Args, typename T, T MemFn>
struct VirtualDispatch<void (C::*)(Args...), T, MemFn> {
  static void Fn(C *obj, Args... args) { (obj->*MemFn)(args...); }
};

template <typename C, typename... Args, typename T, T MemFn>
struct VirtualDispatch<void (C::*)(Args...) const, T, MemFn> {
  static void Fn(C *obj, Args... args) { (obj->*MemFn)(args...); }
};

//===----------------------------------------------------------------------===//
// MemFn returns a function pointer for the provided method/function. For
// regular non-virtual methods and functions, this is easy and specified in the
// Itanium ABI. For virtual functions, we use a virtual dispatch thunk (above),
// to handle it and return the address of the thunk.
//
// NOTE: This only works on compilers adhering to the Itanium ABI. This includes
//       Clang and GCC, the ones we currently care about. On Windows, this won't
//       work.
// NOTE: ^^^ That isn't entirely true, we also make assumptions about pointer
//       sizes and casting rules that are not entirely standards-compliant.
//===----------------------------------------------------------------------===//

template <typename X, typename T, T t>
struct MemFn;

template <typename R, typename C, typename... Args, typename T, T F>
struct MemFn<R (C::*)(Args...), T, F> {
  static void *Get() {
    union VoidCast {
      T f;
      struct {
        void *raw_func;
        ptrdiff_t adjustment;
      } d;
      struct {
        ptrdiff_t offset;
        ptrdiff_t adjustment;
      } v;
      static_assert(sizeof(f) == sizeof(v), "");
      static_assert(sizeof(v) == sizeof(d), "");
    };

    // Do the cast
    static constexpr VoidCast cast = {.f = F};

    // If the function is not virtual, we're good
    if (reinterpret_cast<ptrdiff_t>(cast.v.offset) % sizeof(ptrdiff_t) == 0) {
      return cast.d.raw_func;
    }

    // The function is virtual, we need special thunk dispatch
    return reinterpret_cast<void *>(detail::VirtualDispatch<T, T, F>::Fn);
  }
};

template <typename R, typename C, typename... Args, typename T, T F>
struct MemFn<R (C::*)(Args...) const, T, F> {
  static void *Get() {
    union VoidCast {
      T f;
      struct {
        void *raw_func;
        ptrdiff_t adjustment;
      } d;
      struct {
        ptrdiff_t offset;
        ptrdiff_t adjustment;
      } v;
      static_assert(sizeof(f) == sizeof(v), "");
      static_assert(sizeof(v) == sizeof(d), "");
    };

    // Do the cast
    static constexpr VoidCast cast = {.f = F};

    // If the function is not virtual, we're good
    if (reinterpret_cast<ptrdiff_t>(cast.v.offset) % sizeof(ptrdiff_t) == 0) {
      return cast.d.raw_func;
    }

    // The function is virtual, we need special thunk dispatch
    return reinterpret_cast<void *>(detail::VirtualDispatch<T, T, F>::Fn);
  }
};

template <typename R, typename... Args, typename T, T F>
struct MemFn<R (*)(Args...), T, F> {
  static void *Get() { return reinterpret_cast<void *>(F); }
};

}  // namespace detail
}  // namespace proxy

#define MEMFN(f) \
  ::peloton::codegen::proxy::detail::MemFn<decltype(f), decltype(f), f>::Get()

// P: Proxy, N: method name, PTR: C++ method pointer, MNAME: mangled name
#define DEFINE_METHOD(P, N, PTR, MNAME)                                       \
  P##Proxy::_##N P##Proxy::N = {};                                            \
  const char *P##Proxy::_##N::k##N##FnName = MNAME;                           \
  ::llvm::Function *P##Proxy::_##N::GetFunction(                              \
      ::peloton::codegen::CodeGen &codegen) const {                           \
    /* If the function has already been defined, return it. */                \
    if (::llvm::Function *func = codegen.LookupBuiltin(k##N##FnName)) {       \
      return func;                                                            \
    }                                                                         \
                                                                              \
    /* Ensure either a function pointer or a member function pointer */       \
    static_assert(                                                            \
        ((::std::is_pointer<decltype(PTR)>::value &&                          \
          ::std::is_function<                                                 \
              typename ::std::remove_pointer<decltype(PTR)>::type>::value) || \
         ::std::is_member_function_pointer<decltype(PTR)>::value),            \
        "You must provide a pointer to the function you want to proxy");      \
                                                                              \
    /* The function hasn't been registered. Do it now. */                     \
    auto *func_type_ptr = ::llvm::cast<::llvm::PointerType>(                  \
        ::peloton::codegen::proxy::TypeBuilder<decltype(PTR)>::GetType(       \
            codegen));                                                        \
    auto *func_type =                                                         \
        ::llvm::cast<::llvm::FunctionType>(func_type_ptr->getElementType());  \
    return codegen.RegisterBuiltin(k##N##FnName, func_type, MEMFN(PTR));      \
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