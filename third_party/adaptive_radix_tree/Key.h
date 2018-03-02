#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <limits>

namespace art {

using KeyLen = uint32_t;

class Key {
 public:
  static constexpr uint32_t defaultLen = 128;
  static constexpr uint64_t maxKeyLen = std::numeric_limits<uint32_t>::max();

 private:
  uint8_t *data;

  uint32_t len = 0;

  uint8_t stackKey[defaultLen];

 public:
  Key() {}

  ~Key();

  Key(const Key &key) = delete;

  Key(Key &&key);

  void setFrom(const Key &o);
  void set(const char bytes[], const std::size_t length);

  void operator=(const char key[]);

  bool operator==(const Key &k) const {
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

inline uint8_t &Key::operator[](std::size_t i) {
  assert(i < len);
  return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
  assert(i < len);
  return data[i];
}

inline KeyLen Key::getKeyLen() const { return len; }

inline Key::~Key() {
  if (len > defaultLen) {
    delete[] data;
    data = nullptr;
  }
}

inline Key::Key(Key &&key) {
  len = key.len;
  if (len > defaultLen) {
    data = key.data;
    key.data = nullptr;
  } else {
    memcpy(stackKey, key.stackKey, key.len);
    data = stackKey;
  }
}

inline void Key::setFrom(const Key &o) {
  set(reinterpret_cast<const char *>(o.data), o.len);
}

inline void Key::set(const char bytes[], const std::size_t length) {
  if (len > defaultLen) {
    delete[] data;
  }
  if (length <= defaultLen) {
    memcpy(stackKey, bytes, length);
    data = stackKey;
  } else {
    data = new uint8_t[length];
    memcpy(data, bytes, length);
  }
  len = length;
}

inline void Key::operator=(const char key[]) {
  if (len > defaultLen) {
    delete[] data;
  }
  len = strlen(key);
  if (len <= defaultLen) {
    memcpy(stackKey, key, len);
    data = stackKey;
  } else {
    data = new uint8_t[len];
    memcpy(data, key, len);
  }
}

inline void Key::setKeyLen(KeyLen newLen) {
  if (len == newLen) return;
  if (len > defaultLen) {
    delete[] data;
  }
  len = newLen;
  if (len > defaultLen) {
    data = new uint8_t[len];
  } else {
    data = stackKey;
  }
}

}  // namespace art
