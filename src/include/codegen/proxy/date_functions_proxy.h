//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_proxy.h
//
// Identification: src/include/codegen/proxy/date_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(DateFunctions) {
  // Proxy everything in function::StringFunctions
  DECLARE_METHOD(Now);
};

}  // namespace codegen
}  // namespace peloton
