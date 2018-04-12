//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter.cpp
//
// Identification: src/codegen/util/bloom_filter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/bloom_filter.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"

#include <cmath>
#include <vector>

#define OPTIMAL_NUM_HASH_FUNC 0

namespace peloton {
namespace codegen {
namespace util {

//===----------------------------------------------------------------------===//
// Static Members
//===----------------------------------------------------------------------===//
const Hash::HashMethod BloomFilter::kSeedHashFuncs[2] = {
    Hash::HashMethod::Murmur3, Hash::HashMethod::Crc32};

const double BloomFilter::kFalsePositiveRate = 0.1;

// Set it to OPTIMAL_NUM_HASH_FUNC to minimize bloom filter memory footprint
const uint64_t BloomFilter::kNumHashFuncs = 1;

//===----------------------------------------------------------------------===//
// Member Functions
//===----------------------------------------------------------------------===//

void BloomFilter::Init(uint64_t estimated_num_tuples) {
  if (kNumHashFuncs == OPTIMAL_NUM_HASH_FUNC) {
    // Calculate the optimal number of hash functions and num of bits that
    // will minimize the bloom filter's memory footprint. Formula is from
    // http://blog.michaelschmatz.com/2016/04/11/how-to-write-a-bloom-filter-cpp/
    num_bits_ = -(double)estimated_num_tuples * log(kFalsePositiveRate) /
                (log(2) * log(2));
    num_hash_funcs_ = (double)num_bits_ * log(2) / (double)estimated_num_tuples;
  } else {
    // Manually set the number of hash functions to use. The memory footprint
    // may not be mininal but the performance could better since the cost of
    // probing bloom filter is O(num_hash_funcs_). Formula is from wikipedia.
    num_hash_funcs_ = kNumHashFuncs;
    num_bits_ = -(double)estimated_num_tuples * num_hash_funcs_ /
                log(1 - pow(kFalsePositiveRate, 1.0 / (double)num_hash_funcs_));
  }
  LOG_INFO("BloomFilter num_bits: %lu bits_per_element: %f num_hash_funcs: %lu",
           (unsigned long)num_bits_, (double)num_bits_ / estimated_num_tuples,
           (unsigned long)num_hash_funcs_);

  // Allocate memory for the underlying bytes array
  uint64_t num_bytes = (num_bits_ + 7) / 8;
  bytes_ = new char[num_bytes];
  PELOTON_MEMSET(bytes_, 0, num_bytes);

  // Initialize Statistics
  num_misses_ = 0;
  num_probes_ = 0;
}

void BloomFilter::Destroy() {
  // Free memory of underlying bytes array
  LOG_DEBUG("Bloom Filter, num_probes: %lu, misses: %lu, Selectivity: %f",
            (unsigned long)num_probes_, (unsigned long)num_misses_,
            (double)(num_probes_ - num_misses_) / num_probes_);
  delete[] bytes_;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
