//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter.h
//
// Identification: include/codegen/bloom_filter.h
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

  // Codegen the bloom filter insert
  static void Add(CodeGen &codegen, llvm::Value *bloom_filter,
                  const std::vector<codegen::Value> &key);

  // Codegen the bloom filter probe
  static llvm::Value *Contains(CodeGen &codegen, llvm::Value *bloom_filter,
                               const std::vector<codegen::Value> &key);

 private:
  static void StoreBloomFilterField(CodeGen &codegen, llvm::Value *bloom_filter,
                                    uint32_t field_id,
                                    llvm::Value *new_field_val);

  static llvm::Value *LoadBloomFilterField(CodeGen &codegen,
                                           llvm::Value *bloom_filter,
                                           uint32_t field_id);

  static llvm::Value *CalculateHash(CodeGen &codegen, llvm::Value *index,
                                    llvm::Value *seed_hash1,
                                    llvm::Value *seed_hash2);

  static void LocateBit(CodeGen &codegen, llvm::Value *bloom_filter,
                        llvm::Value *hash, llvm::Value *&bit_offset_in_byte,
                        llvm::Value *&byte_ptr);

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

}  // namespace codegen
}  // namespace peloton
