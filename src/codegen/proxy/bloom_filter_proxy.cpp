//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.cpp
//
// Identification: src/codegen/proxy/bloom_filter_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/bloom_filter_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(BloomFilter, "peloton::BloomFilter", num_hash_funcs, bytes,
            num_bits, num_misses, num_probes);

DEFINE_METHOD(peloton::codegen::util, BloomFilter, Init);
DEFINE_METHOD(peloton::codegen::util, BloomFilter, Destroy);

}  // namespace codegen
}  // namespace peloton