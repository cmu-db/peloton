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
  // Hash functions used in bloom filters
  static std::vector<Hash::HashMethod> kHashFunctions;

 public:
  void Init(CodeGen &codegen, llvm::Value *bloom_filter);

  void Destroy(CodeGen &codegen, llvm::Value *bloom_filter);

  void Add(CodeGen &codegen, llvm::Value *bloom_filter,
           std::vector<codegen::Value> &key) const;

  llvm::Value *Contains(CodeGen &codegen, llvm::Value *bloom_filter,
                        std::vector<codegen::Value> &key) const;

 private:
  llvm::Value *CalculateHashes(CodeGen &codegen,
                               std::vector<codegen::Value> &key) const;
};

}  // namespace codegen
}  // namespace peloton
