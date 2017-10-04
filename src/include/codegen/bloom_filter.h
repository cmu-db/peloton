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
  static const double false_positive_rate;

 public:
  void Init(CodeGen &codegen, llvm::Value *bloom_filter,
            uint64_t estimated_number_tuples);

  void Destroy(CodeGen &codegen, llvm::Value *bloom_filter);

  void Add(CodeGen &codegen, llvm::Value *bloom_filter,
           const std::vector<codegen::Value> &key) const;

  llvm::Value *Contains(CodeGen &codegen, llvm::Value *bloom_filter,
                        const std::vector<codegen::Value> &key) const;

 private:
  llvm::Value *CalculateHashes(CodeGen &codegen,
                               const std::vector<codegen::Value> &key) const;

 private:
  int num_hash_funcs_;
};

}  // namespace codegen
}  // namespace peloton
