//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer_proxy.cpp
//
// Identification: src/codegen/proxy/item_pointer_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/item_pointer_proxy.h"

namespace peloton {
namespace codegen {
DEFINE_TYPE(ItemPointer, "ItemPointer", MEMBER(opaque));
}
}
