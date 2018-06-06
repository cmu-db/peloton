//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_proxy.h
//
// Identification: src/include/codegen/proxy/tuple_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "storage/tuple.h"

namespace peloton {
namespace codegen {

PROXY(Tuple) {
  DECLARE_MEMBER(0, char[sizeof(storage::Tuple)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Tuple, storage::Tuple);

}  // namespace codegen
}  // namespace peloton
