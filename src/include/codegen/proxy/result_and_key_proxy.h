//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.h
//
// Identification: include/codegen/proxy/result_and_key_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "index/art_index.h"
#include "codegen/proxy/item_pointer_proxy.h"
#include "codegen/proxy/art_key_proxy.h"

namespace peloton {
namespace codegen {
PROXY(ResultAndKey) {
  DECLARE_MEMBER(0, ItemPointer*, tuple_p);
  DECLARE_MEMBER(1, index::ARTKey, continue_key);

  DECLARE_TYPE;
};

TYPE_BUILDER(ResultAndKey, index::ResultAndKey);
}
}
