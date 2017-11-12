//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/N4.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_N4_H
#define PELOTON_N4_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/Key.h"
#include "index/Epoche.h"

namespace peloton {
namespace index {
class N4 : public N {
public:
  uint8_t keys[4];
  N *children[4] = {nullptr, nullptr, nullptr, nullptr};

public:
  N4(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N4, prefix,
                                                       prefixLength) { }

  void insert(uint8_t key, N *n);

  template<class NODE>
  void copyTo(NODE *n) const {
    for (uint32_t i = 0; i < count; ++i) {
      n->insert(keys[i], children[i]);
    }
  }

  bool change(uint8_t key, N *val);

  bool addMultiValue(uint8_t key, uint64_t val);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount) const;
};
}
}

#endif //PELOTON_N4_H
