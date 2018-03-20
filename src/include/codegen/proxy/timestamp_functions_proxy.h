//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_proxy.h
//
// Identification: src/include/codegen/proxy/timestamp_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(TimestampFunctions) {
  // Proxy everything in function::DateFunctions
  DECLARE_METHOD(DateTrunc);
  DECLARE_METHOD(DatePart);
};

}  // namespace codegen
}  // namespace peloton
