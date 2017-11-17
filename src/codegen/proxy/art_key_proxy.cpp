//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_key_proxy.cpp
//
// Identification: src/codegen/proxy/art_key_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/art_key_proxy.h"

namespace peloton {
namespace codegen {
DEFINE_TYPE(ARTKey, "index::ARTKey", MEMBER(opaque));
}
}