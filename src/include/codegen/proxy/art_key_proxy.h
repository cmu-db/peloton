//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_key_proxy.h
//
// Identification: src/include/codegen/proxy/art_key_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "index/index.h"
#include "index/art_key.h"
#include "index/art_index.h"

namespace peloton {
namespace codegen {
PROXY(ARTKey) {
  DECLARE_MEMBER(0, char[sizeof(index::ARTKey)], opaque);
  DECLARE_TYPE;

//  DECLARE_METHOD(CodeGenScan);
};

TYPE_BUILDER(ARTKey, index::ARTKey);
}
}