//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_node_256_node.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_N256_H
#define PELOTON_N256_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/art_key.h"
#include "index/art_epoch_manager.h"

namespace peloton {
namespace index {
class N256 : public N {
  N *children[256];

 public:
  N256(const uint8_t *prefix, uint32_t prefixLength)
      : N(NTypes::N256, prefix, prefixLength) {
    memset(children, '\0', sizeof(children));
  }

  void insert(uint8_t key, N *val);

  template <class NODE>
  void copyTo(NODE *n) const {
    for (int i = 0; i < 256; ++i) {
      if (children[i] != nullptr) {
        n->insert(i, children[i]);
      }
    }
  }

  bool Change(uint8_t key, N *n);

  bool AddMultiValue(uint8_t key, uint64_t n);

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

#endif  // PELOTON_N256_H
