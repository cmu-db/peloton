//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_storage_proxy.h
//
// Identification: include/codegen/proxy/bloom_filter_storage_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/util/bloom_filter_storage.h"

namespace peloton {
namespace codegen {

PROXY(BloomFilterStorage) {
  DECLARE_MEMBER(0, char[sizeof(util::BloomFilterStorage)], opaque);

  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(Add);
  DECLARE_METHOD(Contains);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(BloomFilterStorage, util::BloomFilterStorage);

}  // namespace codegen
}  // namespace peloton