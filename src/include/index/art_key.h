//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/Key.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_KEY_H
#define PELOTON_KEY_H

#pragma once

#include <stdint.h>
#include <cstring>
#include <memory>
#include <assert.h>

namespace peloton {
namespace index {
using KeyLen = uint32_t;

class ARTKey {
 public:
  static constexpr uint32_t stackLen = 128;
  uint32_t len = 0;

  uint8_t *data;

  uint8_t stackKey[stackLen];

  ARTKey(uint64_t k) { setInt(k); }

  void setInt(uint64_t k) {
    data = stackKey;
    len = 8;
    *reinterpret_cast<uint64_t *>(stackKey) = __builtin_bswap64(k);
  }

  ARTKey() {}

  ~ARTKey();

  ARTKey(const ARTKey &key) = delete;

  ARTKey(ARTKey &&key);

  void set(const char bytes[], const std::size_t length);

  void operator=(const char key[]);

  bool operator==(const ARTKey &k) const {
    if (k.getKeyLen() != getKeyLen()) {
      return false;
    }
    return std::memcmp(&k[0], data, getKeyLen()) == 0;
  }

  uint8_t &operator[](std::size_t i);

  const uint8_t &operator[](std::size_t i) const;

  KeyLen getKeyLen() const;

  void setKeyLen(KeyLen len);
};

inline uint8_t &ARTKey::operator[](std::size_t i) {
  assert(i < len);
  return data[i];
}

inline const uint8_t &ARTKey::operator[](std::size_t i) const {
  assert(i < len);
  return data[i];
}

inline KeyLen ARTKey::getKeyLen() const { return len; }

inline ARTKey::~ARTKey() {
  if (len > stackLen) {
    delete[] data;
    data = nullptr;
  }
}

inline ARTKey::ARTKey(ARTKey &&key) {
  len = key.len;
  if (len > stackLen) {
    data = key.data;
    key.data = nullptr;
  } else {
    memcpy(stackKey, key.stackKey, key.len);
    data = stackKey;
  }
}

inline void ARTKey::set(const char bytes[], const std::size_t length) {
  if (len > stackLen) {
    delete[] data;
  }
  if (length <= stackLen) {
    memcpy(stackKey, bytes, length);
    data = stackKey;
  } else {
    data = new uint8_t[length];
    memcpy(data, bytes, length);
  }
  len = length;
}

inline void ARTKey::operator=(const char key[]) {
  if (len > stackLen) {
    delete[] data;
  }
  len = strlen(key);
  if (len <= stackLen) {
    memcpy(stackKey, key, len);
    data = stackKey;
  } else {
    data = new uint8_t[len];
    memcpy(data, key, len);
  }
}

inline void ARTKey::setKeyLen(KeyLen newLen) {
  if (len == newLen) return;
  if (len > stackLen) {
    delete[] data;
  }
  len = newLen;
  if (len > stackLen) {
    data = new uint8_t[len];
  } else {
    data = stackKey;
  }
}
}
}

#endif  // PELOTON_KEY_H
