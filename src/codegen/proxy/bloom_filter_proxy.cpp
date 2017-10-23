//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.cpp
//
// Identification: src/codegen/proxy/bloom_filter_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/bloom_filter_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(BloomFilter, "peloton::BloomFilter", MEMBER(num_hash_funcs),
            MEMBER(bytes), MEMBER(num_bits), MEMBER(num_misses),
            MEMBER(num_probes));

DEFINE_METHOD(peloton::codegen::util, BloomFilter, Init);
DEFINE_METHOD(peloton::codegen::util, BloomFilter, Destroy);

}  // namespace codegen
}  // namespace peloton