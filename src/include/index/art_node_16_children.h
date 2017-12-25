//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_node_16_node.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_N16_H
#define PELOTON_N16_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/art_key.h"
#include "index/art_epoch_manager.h"

namespace peloton {
namespace index {
class N16 : public N {
 public:
  uint8_t keys[16];
  N *children[16];

  static uint8_t flipSign(uint8_t keyByte) {
    // Flip the sign bit, enables signed SSE comparison of unsigned values, used
    // by Node16
    return keyByte ^ 128;
  }

  static inline unsigned ctz(uint16_t x) {
// Count trailing zeros, only defined for x>0
#ifdef __GNUC__
    return __builtin_ctz(x);
#else
    // Adapted from Hacker's Delight
    unsigned n = 1;
    if ((x & 0xFF) == 0) {
      n += 8;
      x = x >> 8;
    }
    if ((x & 0x0F) == 0) {
      n += 4;
      x = x >> 4;
    }
    if ((x & 0x03) == 0) {
      n += 2;
      x = x >> 2;
    }
    return n - (x & 1);
#endif
  }

  N *const *GetChildPos(const uint8_t k) const;

 public:
  N16(const uint8_t *prefix, uint32_t prefixLength)
      : N(NTypes::N16, prefix, prefixLength) {
    memset(keys, 0, sizeof(keys));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, N *n);

  template <class NODE>
  void copyTo(NODE *n) const {
    for (unsigned i = 0; i < count_; i++) {
      n->insert(flipSign(keys[i]), children[i]);
    }
  }

  bool Change(uint8_t key, N *val);

  bool AddMultiValue(uint8_t key, uint64_t val);

  N *GetChild(const uint8_t k) const;

  void remove(uint8_t k, ThreadInfo &thread_info);

  N *GetAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void DeleteChildren();

  uint64_t GetChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount) const;
};
}
}

#endif  // PELOTON_N16_H
