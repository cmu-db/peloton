//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter.h
//
// Identification: src/include/codegen/util/bloom_filter.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/hash.h"

namespace peloton {
namespace codegen {
namespace util {

class BloomFilter {
 public:
  // BloomFilter requires k hashes. For efficiency reason, only the first two
  // seed hashes are generated using general hash function like Murmur3. The
  // rest of hashes will be generated using formula "hashes[i] = hashes[0]
  // + i * hashes[1]". For more details google "Less Hashing, Same Performance:
  // Building a Better Bloom Filter"

  // Seed Hash Functions to use.
  static const Hash::HashMethod kSeedHashFuncs[2];
  // Bloom Filter False Positive Rate
  static const double kFalsePositiveRate;
  // Number of hash functions to use.
  static const uint64_t kNumHashFuncs;

 public:
  // Initialize bloom filter states
  void Init(uint64_t estimated_number_tuples);

  // Destroy the bloom filter states
  void Destroy();

 private:
  // Number of hash functions to use
  uint64_t num_hash_funcs_;

  // The underlying bytes array that store all the bloom filter bits
  char *bytes_;

  // The capacity of the underlying bit array
  uint64_t num_bits_;

  // Statistic: number of misses
  uint64_t num_misses_;

  // Statistic: number of probes
  uint64_t num_probes_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton
