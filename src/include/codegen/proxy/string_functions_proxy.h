//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_proxy.h
//
// Identification: src/include/codegen/proxy/string_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/proxy/type_builder.h"
#include "function/string_functions.h"

namespace peloton {
namespace codegen {

PROXY(StringFunctions) {
  // Proxy everything in function::StringFunctions
  DECLARE_METHOD(Ascii);
  DECLARE_METHOD(Like);
  DECLARE_METHOD(Length);
  DECLARE_METHOD(BTrim);
  DECLARE_METHOD(Trim);
  DECLARE_METHOD(LTrim);
  DECLARE_METHOD(RTrim);
  DECLARE_METHOD(Substr);
  DECLARE_METHOD(Repeat);
};

PROXY(StrWithLen) {
  DECLARE_MEMBER(0, char*, str);
  DECLARE_MEMBER(1, uint32_t, length);
  DECLARE_TYPE;
};

TYPE_BUILDER(StrWithLen, peloton::function::StringFunctions::StrWithLen);

}  // namespace codegen
}  // namespace peloton
