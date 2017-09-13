//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_util.h
//
// Identification: src/include/util/hash_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <algorithm>
#include <string>

namespace peloton {

using hash_t = std::size_t;

class HashUtil {

 private:
  const static hash_t prime_factor = 10000019;

 public:
/* Taken from
 * https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
 */
  static inline hash_t HashBytes(const char *bytes, size_t length) {
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  static inline hash_t CombineHashes(hash_t l, hash_t r) {
    hash_t both[2];
    both[0] = l;
    both[1] = r;
    return HashBytes((char *) both, sizeof(hash_t) * 2);
  }

  static inline hash_t SumHashes(hash_t l, hash_t r) {
    return (l % prime_factor + r % prime_factor) % prime_factor;
  }

  template<typename T>
  static inline hash_t Hash(const T *ptr) {
    return HashBytes((char *) ptr, sizeof(T));
  }

  template<typename T>
  static inline hash_t HashPtr(const T *ptr) {
    return HashBytes((char *) &ptr, sizeof(void *));
  }
};


}  // namespace peloton
