//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter.cpp
//
// Identification: src/codegen/bloom_filter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/bloom_filter.h"
#include "codegen/proxy/bloom_filter_storage_proxy.h"

#include <cmath>

namespace peloton {
namespace codegen {
//===----------------------------------------------------------------------===//
// Static Members
//===----------------------------------------------------------------------===//
const Hash::HashMethod BloomFilter::kSeedHashFuncs[2] = {
    Hash::HashMethod::Murmur3, Hash::HashMethod::Crc32};

const double BloomFilter::false_positive_rate = 0.1;

//===----------------------------------------------------------------------===//
// Member Functions
//===----------------------------------------------------------------------===//

void BloomFilter::Init(CodeGen &codegen, llvm::Value *bloom_filter,
                       uint64_t estimated_num_tuples) {
  // Calculate the size of bloom filter and number of hashes to use based on
  // estimated cardinalities and target false positive rate. Formula is from
  // http://blog.michaelschmatz.com/2016/04/11/how-to-write-a-bloom-filter-cpp/
  uint64_t num_bits = -(double)estimated_num_tuples * log(false_positive_rate) /
                      (log(2) * log(2));
  num_hash_funcs_ = (double)num_bits * log(2) / (double)estimated_num_tuples;
  LOG_DEBUG("BloomFilter num_bits: %lu num_hash_funcs: %d", num_bits,
            num_hash_funcs_);
  PL_ASSERT(num_hash_funcs_ > 0);
  codegen.Call(BloomFilterStorageProxy::Init,
               {bloom_filter, codegen.Const64(num_bits)});
}

void BloomFilter::Destroy(CodeGen &codegen, llvm::Value *bloom_filter) {
  codegen.Call(BloomFilterStorageProxy::Destroy, {bloom_filter});
}

void BloomFilter::Add(CodeGen &codegen, llvm::Value *bloom_filter,
                      const std::vector<codegen::Value> &key) const {
  llvm::Value *num_hashes = codegen.Const32(num_hash_funcs_);
  llvm::Value *hashes = CalculateHashes(codegen, key);
  // Pass the hashes to the underlying bloom filter storage to mark the bits
  codegen.Call(BloomFilterStorageProxy::Add,
               {bloom_filter, hashes, num_hashes});
}

llvm::Value *BloomFilter::Contains(
    CodeGen &codegen, llvm::Value *bloom_filter,
    const std::vector<codegen::Value> &key) const {
  llvm::Value *num_hashes = codegen.Const32(num_hash_funcs_);
  llvm::Value *hashes = CalculateHashes(codegen, key);
  // Pass the hashes to the underlying bloom filter storage to mark the bits
  return codegen.Call(BloomFilterStorageProxy::Contains,
                      {bloom_filter, hashes, num_hashes});
}

llvm::Value *BloomFilter::CalculateHashes(
    CodeGen &codegen, const std::vector<codegen::Value> &key) const {
  // Alloca space on stack to store the hashes
  llvm::Value *hashes = codegen->CreateAlloca(codegen.Int64Type(),
                                              codegen.Const32(num_hash_funcs_));
  std::vector<llvm::Value *> seed_hashes(2);
  for (int i = 0; i < num_hash_funcs_; i++) {
    llvm::Value *hash;
    if (i <= 1) {
      // The first two hashes are calculated using the seed hash functions.
      hash = Hash::HashValues(codegen, key, kSeedHashFuncs[i]);
      seed_hashes[i] = hash;
    } else {
      // For later hashes, we use double hashing to efficient generate hash.
      // hash[i] = hash[0] + i * hash[1];
      // More details can be found http://spyced.blogspot.com/2009/01/all-you-
      // ever-wanted-to-know-about.html
      hash = codegen->CreateAdd(
          seed_hashes[0],
          codegen->CreateMul(codegen.Const64(i), seed_hashes[1]));
    }

    // hashes[i] = hash
    codegen->CreateStore(hash,
                         codegen->CreateInBoundsGEP(codegen.Int64Type(), hashes,
                                                    codegen.Const32(i)));
  }
  return hashes;
}

}  // namespace codegen
}  // namespace peloton
