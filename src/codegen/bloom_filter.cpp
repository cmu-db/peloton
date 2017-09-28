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

namespace peloton {
namespace codegen {
//===----------------------------------------------------------------------===//
// Static Members
//===----------------------------------------------------------------------===//
std::vector<Hash::HashMethod> BloomFilter::kHashFunctions = {
    Hash::HashMethod::Murmur3, Hash::HashMethod::Crc32};

//===----------------------------------------------------------------------===//
// Member Functions
//===----------------------------------------------------------------------===//

void BloomFilter::Init(CodeGen &codegen, llvm::Value *bloom_filter) {
  codegen.Call(BloomFilterStorageProxy::Init, {bloom_filter});
}

void BloomFilter::Destroy(CodeGen &codegen, llvm::Value *bloom_filter) {
  codegen.Call(BloomFilterStorageProxy::Destroy, {bloom_filter});
}

void BloomFilter::Add(CodeGen &codegen, llvm::Value *bloom_filter,
                      const std::vector<codegen::Value> &key) const {
  llvm::Value *num_hashes = codegen.Const32(kHashFunctions.size());
  llvm::Value *hashes = CalculateHashes(codegen, key);
  // Pass the hashes to the underlying bloom filter storage to mark the bits
  codegen.Call(BloomFilterStorageProxy::Add,
               {bloom_filter, hashes, num_hashes});
}

llvm::Value *BloomFilter::Contains(
    CodeGen &codegen, llvm::Value *bloom_filter,
    const std::vector<codegen::Value> &key) const {
  llvm::Value *num_hashes = codegen.Const32(kHashFunctions.size());
  llvm::Value *hashes = CalculateHashes(codegen, key);
  // Pass the hashes to the underlying bloom filter storage to mark the bits
  return codegen.Call(BloomFilterStorageProxy::Contains,
                      {bloom_filter, hashes, num_hashes});
}

llvm::Value *BloomFilter::CalculateHashes(
    CodeGen &codegen, const std::vector<codegen::Value> &key) const {
  // Alloca space on stack to store the hashes
  llvm::Value *hashes = codegen->CreateAlloca(
      codegen.Int64Type(), codegen.Const32(kHashFunctions.size()));
  for (unsigned i = 0; i < kHashFunctions.size(); i++) {
    llvm::Value *hash = Hash::HashValues(codegen, key, kHashFunctions[i]);
    // hashes[i] = hash
    codegen->CreateStore(hash,
                         codegen->CreateInBoundsGEP(codegen.Int64Type(), hashes,
                                                    codegen.Const32(i)));
  }
  return hashes;
}

}  // namespace codegen
}  // namespace peloton
