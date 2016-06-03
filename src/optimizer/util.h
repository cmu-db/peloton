//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.h
//
// Identification: src/backend/optimizer/util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>

namespace peloton {
namespace optimizer {

using hash_t = std::size_t;

namespace util {

/* Taken from
 * https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
 */
inline hash_t HashBytes(const char *bytes, size_t length) {
  hash_t hash = length;
  for(size_t i = 0; i < length; ++i)
  {
    hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
  }
  return hash;
}

inline hash_t CombineHashes(hash_t l, hash_t r) {
  hash_t both[2];
  both[0] = l;
  both[1] = r;
  return HashBytes((char *)both, sizeof(hash_t) * 2);
}


template <typename T>
inline hash_t Hash(const T *ptr) {
  return HashBytes((char *)ptr, sizeof(T));
}

template <typename T>
inline hash_t HashPtr(const T *ptr) {
  return HashBytes((char *)&ptr, sizeof(void *));
}

} /* namespace util */
} /* namespace optimizer */
} /* namespace peloton */
