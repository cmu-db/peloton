//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/N256.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include "index/N.h"
#include "index/N256.h"

namespace peloton {
namespace index {
bool N256::isFull() const {
  return false;
}

bool N256::isUnderfull() const {
  return count == 37;
}

void N256::deleteChildren() {
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      N::deleteChildren(children[i]);
      N::deleteNode(children[i]);
    }
  }
}

void N256::insert(uint8_t key, N *val) {
  children[key] = val;
  count++;
}

bool N256::change(uint8_t key, N *n) {
  children[key] = n;
  return true;
}

bool N256::addMultiValue(uint8_t key, uint64_t val) {
//  children[key] = n;
  TID tid = N::getLeaf(children[key]);

  MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
  while (value_list->next != 0) {
    value_list = (MultiValues *)(value_list->next.load());
  }
  MultiValues *new_value = new MultiValues();
  new_value->tid = val;
  new_value->next = 0;
  value_list->next = (uint64_t) new_value;
  return true;
}

N *N256::getChild(const uint8_t k) const {
  return children[k];
}

void N256::remove(uint8_t k) {
  children[k] = nullptr;
  count--;
}

N *N256::getAnyChild() const {
  N *anyChild = nullptr;
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      if (N::isLeaf(children[i])) {
        return children[i];
      } else {
        anyChild = children[i];
      }
    }
  }
  return anyChild;
}

uint64_t N256::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                           uint32_t &childrenCount) const {
  restart:
  bool needRestart = false;
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->children[i] != nullptr) {
      children[childrenCount] = std::make_tuple(i, this->children[i]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}