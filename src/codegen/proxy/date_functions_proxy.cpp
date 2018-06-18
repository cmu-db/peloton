//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_proxy.cpp
//
// Identification: src/codegen/proxy/date_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/date_functions_proxy.h"

#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/type_builder.h"
#include "function/date_functions.h"

namespace peloton {
namespace codegen {

// Utility functions
DEFINE_METHOD(peloton::function, DateFunctions, Now);

// Input functions
DEFINE_METHOD(peloton::function, DateFunctions, InputDate);

}  // namespace codegen
}  // namespace peloton
