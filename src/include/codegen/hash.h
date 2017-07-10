//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash.h
//
// Identification: src/include/codegen/hash.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Utility class to help compute/codegen the hash of values
//===----------------------------------------------------------------------===//
class Hash {
 private:
  //===--------------------------------------------------------------------===//
  // A varlen is a private struct that serves as a convenience container for a
  // variable length value and its length.  It isn't different than a std::pair
  // but the ordering is clear.
  //===--------------------------------------------------------------------===//
  struct Varlen {
    llvm::Value *val;
    llvm::Value *len;
    Varlen(llvm::Value *_val, llvm::Value *_len) : val(_val), len(_len) {}
  };

 public:
  //===--------------------------------------------------------------------===//
  // The hash methods we currently support
  //===--------------------------------------------------------------------===//
  enum class HashMethod : uint32_t { Crc32, Murmur3, City, Multiplicative };
  static const std::string kHashMethodStrings[4];

  // Given a collection of values, produce a single hash value
  static llvm::Value *HashValues(
      CodeGen &codegen, const std::vector<codegen::Value> &vals,
      Hash::HashMethod method = Hash::HashMethod::Crc32);

 private:
  // Generate the calculation of a CRC64 hash for the given values
  static llvm::Value *ComputeCRC32Hash(
      CodeGen &codegen, const std::vector<llvm::Value *> &numerics,
      const std::vector<Hash::Varlen> &buffers);

  static llvm::Value *ComputeMurmur3Hash(
      CodeGen &codegen, const std::vector<llvm::Value *> &numerics,
      const std::vector<Hash::Varlen> &varlen_buffers);
};

}  // namespace codegen
}  // namespace peloton