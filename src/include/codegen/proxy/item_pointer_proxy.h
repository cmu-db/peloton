//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer_proxy.h
//
// Identification: src/include/codegen/proxy/item_pointer_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "common/item_pointer.h"
#include "index/index.h"

namespace peloton {
namespace codegen {
PROXY(ItemPointer) {
  DECLARE_MEMBER(0, char[sizeof(ItemPointer)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(ItemPointer, ItemPointer);
}
}