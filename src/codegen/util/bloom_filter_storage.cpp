//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_storage.cpp
//
// Identification: src/codegen/util/bloom_filter_storage.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "codegen/util/bloom_filter_storage.h"
#include "common/logger.h"

#include <string.h>

namespace peloton {
namespace codegen {
namespace util {

void BloomFilterStorage::Init() {
  // Allocate enough bytes to all the bits.
  int num_bytes = (kNumBits + 7) / 8;
  bytes_ = new char[num_bytes];
  memset(bytes_, 0, num_bytes);
}

void BloomFilterStorage::Add(uint64_t* hashes, uint32_t size) {
  LOG_INFO("BloomFilterStorage::Add(%lu, %d)", hashes[0], size);
  for (unsigned i = 0; i < size; i++) {
    uint32_t bit_offset = hashes[i] % kNumBits;
    uint32_t byte_offset = bit_offset / 8;
    bit_offset = bit_offset % 8;
    bytes_[byte_offset] |= (0x1 << bit_offset);
  }
}

bool BloomFilterStorage::Contains(uint64_t* hashes, uint32_t size) {
  LOG_INFO("BloomFilterStorage::Contains(%lu, %d)", hashes[0], size);
  for (unsigned i = 0; i < size; i++) {
    uint32_t bit_offset = hashes[i] % kNumBits;
    uint32_t byte_offset = bit_offset / 8;
    bit_offset = bit_offset % 8;
    if ((bytes_[byte_offset] & (0x1 << bit_offset)) == 0) {
      return false;
    }
  }
  return true;
}

void BloomFilterStorage::Destroy() { delete[] bytes_; }

}  // namespace util
}  // namespace codegen
}  // namespace peloton