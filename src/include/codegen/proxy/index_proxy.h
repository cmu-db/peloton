//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_proxy.h
//
// Identification: src/include/codegen/proxy/index_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "index/index.h"

namespace peloton {
namespace codegen {
PROXY(Index) {
  DECLARE_MEMBER(0, char[sizeof(index::Index)], opaque);
  DECLARE_TYPE;

//  DECLARE_METHOD(CodeGenScan);
};

TYPE_BUILDER(Index, index::Index);

}
}
