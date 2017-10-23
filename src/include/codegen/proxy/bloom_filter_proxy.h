//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.h
//
// Identification: include/codegen/proxy/bloom_filter_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/bloom_filter.h"
#include "codegen/hash.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(BloomFilter) {
  // Member Variables
  DECLARE_MEMBER(0, uint64_t, num_hash_funcs_);
  DECLARE_MEMBER(1, char*, bytes_);
  DECLARE_MEMBER(2, uint64_t, num_bits_);
  DECLARE_MEMBER(3, uint64_t, num_misses_);
  DECLARE_MEMBER(4, uint64_t, num_probes_);

  DECLARE_TYPE;

  // Methods
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(BloomFilter, BloomFilter);

}  // namespace codegen
}  // namespace peloton