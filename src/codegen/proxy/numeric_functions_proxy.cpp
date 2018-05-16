//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_functions_proxy.cpp
//
// Identification: src/codegen/proxy/numeric_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/numeric_functions_proxy.h"

#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/type_builder.h"
#include "function/numeric_functions.h"

namespace peloton {
namespace codegen {

// Utility functions
DEFINE_METHOD(peloton::function, NumericFunctions, Abs);
DEFINE_METHOD(peloton::function, NumericFunctions, Floor);
DEFINE_METHOD(peloton::function, NumericFunctions, Round);
DEFINE_METHOD(peloton::function, NumericFunctions, Ceil);

// Input functions
DEFINE_METHOD(peloton::function, NumericFunctions, InputBoolean);
DEFINE_METHOD(peloton::function, NumericFunctions, InputTinyInt);
DEFINE_METHOD(peloton::function, NumericFunctions, InputSmallInt);
DEFINE_METHOD(peloton::function, NumericFunctions, InputInteger);
DEFINE_METHOD(peloton::function, NumericFunctions, InputBigInt);
DEFINE_METHOD(peloton::function, NumericFunctions, InputDecimal);

}  // namespace codegen
}  // namespace peloton
