//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions_proxy.h
//
// Identification: src/include/codegen/proxy/decimal_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(DecimalFunctions) {
  // Proxy everything in function::DecimalFunctions

  DECLARE_METHOD(Floor);
  DECLARE_METHOD(Round);
  DECLARE_METHOD(Ceil);
};

}  // namespace codegen
}  // namespace peloton
