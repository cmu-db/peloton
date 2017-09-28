//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_storage_proxy.cpp
//
// Identification: src/codegen/proxy/bloom_filter_storage_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/bloom_filter_storage_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(BloomFilterStorage, "peloton::BloomFilterStorage", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen::util, BloomFilterStorage, Init);
DEFINE_METHOD(peloton::codegen::util, BloomFilterStorage, Add);
DEFINE_METHOD(peloton::codegen::util, BloomFilterStorage, Contains);
DEFINE_METHOD(peloton::codegen::util, BloomFilterStorage, Destroy);

}  // namespace codegen
}  // namespace peloton