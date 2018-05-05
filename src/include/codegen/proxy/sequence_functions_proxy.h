//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_proxy.h
//
// Identification: src/include/codegen/proxy/string_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "function/sequence_functions.h"

namespace peloton {
namespace codegen {

PROXY(SequenceFunctions) {
  DECLARE_METHOD(Nextval);
  DECLARE_METHOD(Currval);
};

}  // namespace codegen
}  // namespace peloton
