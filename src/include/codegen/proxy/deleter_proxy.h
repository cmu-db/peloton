//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter_proxy.h
//
// Identification: src/include/codegen/proxy/deleter_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/deleter.h"

namespace peloton {
namespace codegen {

PROXY(Deleter) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(Deleter)], opaque);
  DECLARE_TYPE;

  /// Proxy Init() and Delete() in codegen::Deleter
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Delete);
};

TYPE_BUILDER(Deleter, codegen::Deleter);

}  // namespace codegen
}  // namespace peloton
