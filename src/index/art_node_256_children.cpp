//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_node_256_children.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include "index/art_node.h"
#include "index/art_node_256_children.h"

namespace peloton {
namespace index {
bool N256::isFull() const { return false; }

bool N256::isUnderfull() const { return count_ == 37; }

void N256::DeleteChildren() {
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      N::DeleteChildren(children[i]);
      N::DeleteNode(children[i]);
    }
  }
}

void N256::insert(uint8_t key, N *val) {
  children[key] = val;
  count_++;
}

bool N256::Change(uint8_t key, N *n) {
  children[key] = n;
  return true;
}

bool N256::AddMultiValue(uint8_t key, uint64_t val) {
  //  children[key] = n;
  TID tid = N::GetLeaf(children[key]);

  MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
  while (value_list->next != 0) {
    value_list = (MultiValues *)(value_list->next.load());
  }
  MultiValues *new_value = new MultiValues();
  new_value->tid = val;
  new_value->next = 0;
  value_list->next = (uint64_t)new_value;
  return true;
}

N *N256::GetChild(const uint8_t k) const { return children[k]; }

void N256::remove(uint8_t k) {
  children[k] = nullptr;
  count_--;
}

N *N256::GetAnyChild() const {
  N *anyChild = nullptr;
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      if (N::IsLeaf(children[i])) {
        return children[i];
      } else {
        anyChild = children[i];
      }
    }
  }
  return anyChild;
}

uint64_t N256::GetChildren(uint8_t start, uint8_t end,
                           std::tuple<uint8_t, N *> *&children,
                           uint32_t &childrenCount) const {
restart:
  bool needRestart = false;
  uint64_t v;
  v = ReadLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->children[i] != nullptr) {
      children[childrenCount] = std::make_tuple(i, this->children[i]);
      childrenCount++;
    }
  }
  ReadUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}