//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_proxy.h
//
// Identification: src/include/codegen/proxy/date_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(DateFunctions) {
  // Utility functions
  DECLARE_METHOD(Now);

  // Input functions
  DECLARE_METHOD(InputDate);
};

}  // namespace codegen
}  // namespace peloton
