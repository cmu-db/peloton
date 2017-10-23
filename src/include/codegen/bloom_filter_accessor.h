//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_accessor.h
//
// Identification: include/codegen/bloom_filter_accessor.h
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

//===----------------------------------------------------------------------===//
// Codegen Accessor to Bloom Filter
//===----------------------------------------------------------------------===//
class BloomFilterAccessor {
 public:
  // Codegen the bloom filter init
  void Init(CodeGen &codegen, llvm::Value *bloom_filter,
            uint64_t estimated_num_tuples) const;

  // Codegen the bloom filter destroy
  void Destroy(CodeGen &codegen, llvm::Value *bloom_filter) const;

  // Codegen the bloom filter insert
  void Add(CodeGen &codegen, llvm::Value *bloom_filter,
           const std::vector<codegen::Value> &key) const;

  // Codegen the bloom filter probe
  llvm::Value *Contains(CodeGen &codegen, llvm::Value *bloom_filter,
                        const std::vector<codegen::Value> &key) const;

 private:
  void StoreBloomFilterField(CodeGen &codegen, llvm::Value *bloom_filter,
                             uint32_t field_id,
                             llvm::Value *new_field_val) const;

  llvm::Value *LoadBloomFilterField(CodeGen &codegen, llvm::Value *bloom_filter,
                                    uint32_t field_id) const;

  llvm::Value *CalculateHash(CodeGen &codegen, llvm::Value *index,
                             llvm::Value *seed_hash1,
                             llvm::Value *seed_hash2) const;

  void LocateBit(CodeGen &codegen, llvm::Value *bloom_filter, llvm::Value *hash,
                 llvm::Value *&bit_offset_in_byte,
                 llvm::Value *&byte_ptr) const;
};

}  // namespace codegen
}  // namespace peloton
