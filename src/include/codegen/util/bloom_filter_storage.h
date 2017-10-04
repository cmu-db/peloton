//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_storage.h
//
// Identification: src/include/codegen/util/bloom_filter_storage.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {
namespace codegen {
namespace util {

class BloomFilterStorage {
 public:
  // Initialize the underlying storage bytes
  void Init(uint64_t num_bits);

  // Given an array of hashes, mark the corresponding bits.
  void Add(uint64_t* hashes, uint32_t size);

  // Given an array of hashes, check if all the corresponding bits are marked.
  bool Contains(uint64_t* hashes, uint32_t size);

  // Free the underlying storage bytes;
  void Destroy();

 private:
  // The underlying bytes array that store all the bloom filter bits
  char* bytes_;

  // The capacity of the underlying bit array
  uint64_t num_bits_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton