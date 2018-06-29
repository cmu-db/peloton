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

#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/variadic/to_seq.hpp>

#include "codegen/codegen.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

/// This file contains several macros that help in creating proxies to C++
/// classes.

#define PROXY(clazz) struct clazz##Proxy

#define DECLARE_MEMBER(slot_, type, name)                                      \
  /*                                                                           \
   * This macro does three things:                                             \
   *   1. It defines a typedef for the C/C++ type of the member.               \
   *   2. It defines a typedef for the slot/position in the struct the member  \
   *      can be found.                                                        \
   *   3. It defines a static member accessor. This will be defined later when \
   *      the whole struct is defined (i.e., in DEFINE_TYPE()).                \
   */                                                                          \
  using name##_type = type;                                                    \
  using name##_slot = std::integral_constant<uint32_t, slot_>;                 \
  static const CppProxyMember name;

#define DECLARE_TYPE \
  static ::llvm::Type *GetType(::peloton::codegen::CodeGen &codegen);

#define DECLARE_METHOD(name)                   \
  struct _##name {                             \
    static ::llvm::Function *GetFunction(      \
        ::peloton::codegen::CodeGen &codegen); \
  };                                           \
  static _##name name;

#define MEMBER_TYPE(r, data, member_name)              \
  ::peloton::codegen::proxy::TypeBuilder<BOOST_PP_CAT( \
      member_name, _type)>::GetType(codegen),

#define DEFINE_MEMBER(r, clazz, member_name)                                 \
  /*                                                                         \
   * Here we define and initialize the proxy member. Each member is          \
   * initialized with the slot/position in the struct it lives in. This slot \
   * position was provided when the member was declared.                     \
   */                                                                        \
  const CppProxyMember BOOST_PP_CAT(                                         \
      clazz, Proxy)::member_name{BOOST_PP_CAT(member_name, _slot)()};

#define DEFINE_TYPE(clazz, str_name, members...)                               \
  /* First we invoke DEFINE_MEMBER() on each proxy member to initialize it */  \
  BOOST_PP_SEQ_FOR_EACH(DEFINE_MEMBER, clazz,                                  \
                        BOOST_PP_VARIADIC_TO_SEQ(members))                     \
                                                                               \
  /* This is the function that constructs the LLVM layout of the proxy type */ \
  ::llvm::Type *clazz##Proxy::GetType(::peloton::codegen::CodeGen &codegen) {  \
    static constexpr const char *kTypeName = str_name;                         \
    /* Check if type has already been registered */                            \
    ::llvm::Type *type = codegen.LookupType(kTypeName);                        \
    if (type != nullptr) {                                                     \
      return type;                                                             \
    }                                                                          \
    /*                                                                         \
     * The type hasn't been registered. We need to construct a vector of       \
     * llvm::Type * that define the layout of the struct/class. To do this, we \
     * iterate over each of the members, find an appropriate TypeBuilder to    \
     * build the LLVM type. We know each member's type because a type-def was  \
     * defined when the member was first declared.                             \
     */                                                                        \
    ::std::vector<::llvm::Type *> member_types = {BOOST_PP_SEQ_FOR_EACH(       \
        MEMBER_TYPE, _, BOOST_PP_VARIADIC_TO_SEQ(members))};                   \
    return ::llvm::StructType::create(codegen.GetContext(), member_types,      \
                                      kTypeName);                              \
  }

namespace proxy {
namespace detail {

//===----------------------------------------------------------------------===//
// The VirtualDispatch templated struct exists to correctly proxy virtual method
// invocations. We need this struct because the address of a virtual method
// pointer cannot be obtained in C++, according to the Itanium ABI (see the
// MemFn struct below to understand why). To get around this, we instantiate a
// static function (with a known address) whose only job is to perform the
// virtual function call given the instance object and arguments.
//
// VirtualDispatch makes it possible to call virtual functions from codegen'd
// code, though this should not be done in a hot-loop!
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
// MemFn returns a function pointer for the provided method/function. For static
// functions and static member methods, this is easy by just casting the pointer
// to a void pointer.
//
// Non-static methods are a little trickier. The Itanium ABI stores
// pointer-to-member functions are a 16-byte pair of 8-byte values:
//   - ptr: If the function is not virtual, this is a regular function pointer.
//          If the function is virtual, this value is one plus the offset of the
//          virtual-table in the class.
//   - adjustment: This is the byte-adjustment to the "this" pointer needed to
//                 point to the correct class in the inheritance hierarchy.
//
// If the function is a regular, non-static, non-virtual method, we just return
// the raw function pointer. If the function is a virtual method, we wrap it in
// a VirtualDispatch thunk.
//
// NOTE: This only works on compilers adhering to the Itanium ABI. This includes
//       Clang and GCC, the ones we currently care about. On Windows, this won't
//       work.
// NOTE: We also make assumptions about pointer sizes and casting rules that are
//       not entirely standards-compliant.
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

template <typename R, typename... Args, typename T, T F>
struct MemFn<R (*)(Args..., ...), T, F> {
  static void *Get() { return reinterpret_cast<void *>(F); }
};

}  // namespace detail
}  // namespace proxy

#define MEMFN(f) \
  ::peloton::codegen::proxy::detail::MemFn<decltype(f), decltype(f), f>::Get()

#define STR(x) #x

#define DEFINE_METHOD(NS, C, F)                                               \
  C##Proxy::_##F C##Proxy::F = {};                                            \
  ::llvm::Function *C##Proxy::_##F::GetFunction(                              \
      ::peloton::codegen::CodeGen &codegen) {                                 \
    static constexpr const char *kFnName = STR(NS::C::F);                     \
    /* If the function has already been defined, return it. */                \
    if (::llvm::Function *func = codegen.LookupBuiltin(kFnName).first) {      \
      return func;                                                            \
    }                                                                         \
                                                                              \
    /* Ensure either a function pointer or a member function pointer */       \
    static_assert(                                                            \
        ((::std::is_pointer<decltype(&NS::C::F)>::value &&                    \
          ::std::is_function<typename ::std::remove_pointer<decltype(         \
              &NS::C::F)>::type>::value) ||                                   \
         ::std::is_member_function_pointer<decltype(&NS::C::F)>::value),      \
        "You must provide a pointer to the function you want to proxy");      \
                                                                              \
    /* The function hasn't been registered. Do it now. */                     \
    auto *func_type_ptr = ::llvm::cast<::llvm::PointerType>(                  \
        ::peloton::codegen::proxy::TypeBuilder<decltype(&NS::C::F)>::GetType( \
            codegen));                                                        \
    auto *func_type =                                                         \
        ::llvm::cast<::llvm::FunctionType>(func_type_ptr->getElementType());  \
    return codegen.RegisterBuiltin(kFnName, func_type, MEMFN(&NS::C::F));     \
  }

#define TYPE_BUILDER(PROXY, TYPE)                                      \
  namespace proxy {                                                    \
  template <>                                                          \
  struct TypeBuilder<TYPE> {                                           \
    static ::llvm::Type *GetType(::peloton::codegen::CodeGen &codegen) \
        ALWAYS_INLINE {                                                \
      return PROXY##Proxy::GetType(codegen);                           \
    }                                                                  \
  };                                                                   \
  }

}  // namespace codegen
}  // namespace peloton