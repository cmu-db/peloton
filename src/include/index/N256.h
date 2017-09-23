//
// Created by Min Huang on 9/21/17.
//

#ifndef PELOTON_N256_H
#define PELOTON_N256_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/Key.h"
#include "index/Epoche.h"

namespace peloton {
namespace index {
class N256 : public N {
  N *children[256];

public:
  N256(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N256, prefix,
                                                         prefixLength) {
    memset(children, '\0', sizeof(children));
  }

  void insert(uint8_t key, N *val);

  template<class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, N *n);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount) const;
};
}
}

#endif //PELOTON_N256_H
