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

DEFINE_TYPE(BloomFilter, "peloton::BloomFilter", MEMBER(num_hash_funcs_),
            MEMBER(bytes_), MEMBER(num_bits_));

DEFINE_METHOD(peloton::codegen, BloomFilter, Init);
DEFINE_METHOD(peloton::codegen, BloomFilter, Destroy);

}  // namespace codegen
}  // namespace peloton