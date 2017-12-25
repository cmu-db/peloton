//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_node_48_node.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef PELOTON_N48_H
#define PELOTON_N48_H

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/art_key.h"
#include "index/art_epoch_manager.h"

namespace peloton {
namespace index {
class N48 : public N {
  uint8_t childIndex[256];
  N *children[48];

 public:
  static const uint8_t emptyMarker = 48;

  N48(const uint8_t *prefix, uint32_t prefixLength)
      : N(NTypes::N48, prefix, prefixLength) {
    memset(childIndex, emptyMarker, sizeof(childIndex));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, N *n);

  template <class NODE>
  void copyTo(NODE *n) const {
    for (unsigned i = 0; i < 256; i++) {
      if (childIndex[i] != emptyMarker) {
        n->insert(i, children[childIndex[i]]);
      }
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

#endif  // PELOTON_N48_H
