//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_proxy.cpp
//
// Identification: src/codegen/proxy/timestamp_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/timestamp_functions_proxy.h"

#include "codegen/codegen.h"
#include "function/timestamp_functions.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::function, TimestampFunctions, DateTrunc);
DEFINE_METHOD(peloton::function, TimestampFunctions, DatePart);

}  // namespace codegen
}  // namespace peloton
