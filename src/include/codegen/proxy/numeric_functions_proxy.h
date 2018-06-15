//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_functions_proxy.h
//
// Identification: src/include/codegen/proxy/numeric_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(NumericFunctions) {
  // Utility functions
  DECLARE_METHOD(Abs);
  DECLARE_METHOD(Floor);
  DECLARE_METHOD(Round);
  DECLARE_METHOD(Ceil);

  // Input functions
  DECLARE_METHOD(InputBoolean);
  DECLARE_METHOD(InputTinyInt);
  DECLARE_METHOD(InputSmallInt);
  DECLARE_METHOD(InputInteger);
  DECLARE_METHOD(InputBigInt);
  DECLARE_METHOD(InputDecimal);
};

}  // namespace codegen
}  // namespace peloton
