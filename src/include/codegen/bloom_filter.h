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
  static const double kFalsePositiveRate;

 public:
  void Init(uint64_t estimated_number_tuples);

  void Destroy();

  static void Add(CodeGen &codegen, llvm::Value *bloom_filter,
                  const std::vector<codegen::Value> &key);

  static llvm::Value *Contains(CodeGen &codegen, llvm::Value *bloom_filter,
                               const std::vector<codegen::Value> &key);

 private:
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
};

}  // namespace codegen
}  // namespace peloton
