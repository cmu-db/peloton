//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions_proxy.cpp
//
// Identification: src/codegen/proxy/decimal_functions_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/decimal_functions_proxy.h"

#include "codegen/proxy/type_builder.h"
#include "function/decimal_functions.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::function, DecimalFunctions, Floor);

DEFINE_METHOD(peloton::function, DecimalFunctions, Round);

DEFINE_METHOD(peloton::function, DecimalFunctions, Ceil);
}  // namespace codegen
}  // namespace peloton
