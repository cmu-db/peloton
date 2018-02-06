//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functor.h
//
// Identification: src/include/codegen/functor.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

namespace peloton {
namespace codegen {

template <typename T> class Functor;

template <typename R, typename ...Args>
class Functor<R(Args...)> {
 public:
  using TypedFunc = R(*)(Args...);

  explicit Functor(TypedFunc raw_func) { fn = raw_func; }

  R operator()(Args... args) const {
    return fn(static_cast<Args &&>(args)...);
  }

 private:
  union {
    TypedFunc fn;
    // Add your stuff here
  };
};

}  // namespace codegen
}  // namespace peloton