//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_proxy.h
//
// Identification: src/include/codegen/proxy/string_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(StringFunctions) {
  // Proxy everything in function::StringFunctions
  DECLARE_METHOD(Ascii);
};

}  // namespace codegen
}  // namespace peloton
