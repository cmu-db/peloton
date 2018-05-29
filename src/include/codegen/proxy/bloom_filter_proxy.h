//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.h
//
// Identification: include/codegen/proxy/bloom_filter_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/util/bloom_filter.h"

namespace peloton {
namespace codegen {

PROXY(BloomFilter) {
  // Member Variables
  DECLARE_MEMBER(0, uint64_t, num_hash_funcs);
  DECLARE_MEMBER(1, char *, bytes);
  DECLARE_MEMBER(2, uint64_t, num_bits);
  DECLARE_MEMBER(3, uint64_t, num_misses);
  DECLARE_MEMBER(4, uint64_t, num_probes);

  DECLARE_TYPE;

  // Methods
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(BloomFilter, util::BloomFilter);

}  // namespace codegen
}  // namespace peloton