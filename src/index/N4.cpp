//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/N4.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include "index/N.h"
#include "index/N4.h"

namespace peloton {
namespace index {
void N4::deleteChildren() {
  for (uint32_t i = 0; i < count; ++i) {
    N::deleteChildren(children[i]);
    N::deleteNode(children[i]);
  }
}

bool N4::isFull() const {
  return count == 4;
}

bool N4::isUnderfull() const {
  return false;
}

void N4::insert(uint8_t key, N *n) {
  unsigned pos;
  for (pos = 0; (pos < count) && (keys[pos] < key); pos++);
  memmove(keys + pos + 1, keys + pos, count - pos);
  memmove(children + pos + 1, children + pos, (count - pos) * sizeof(N*));
  keys[pos] = key;
  children[pos] = n;
  count++;
}

bool N4::change(uint8_t key, N *val) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == key) {
      children[i] = val;
      return true;
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N4::addMultiValue(uint8_t key, uint64_t val) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == key) {
//      children[i] = val;
      TID tid = N::getLeaf(children[i]);

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
  }
  assert(false);
  __builtin_unreachable();
}

N *N4::getChild(const uint8_t k) const {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == k) {
      return children[i];
    }
  }
  return nullptr;
}

void N4::remove(uint8_t k) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == k) {
      memmove(keys + i, keys + i + 1, count - i - 1);
      memmove(children + i, children + i + 1, (count - i - 1) * sizeof(N *));
      count--;
      return;
    }
  }
}

N *N4::getAnyChild() const {
  N *anyChild = nullptr;
  for (uint32_t i = 0; i < count; ++i) {
    if (N::isLeaf(children[i])) {
      return children[i];
    } else {
      anyChild = children[i];
    }
  }
  return anyChild;
}

std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] != key) {
      return std::make_tuple(children[i], keys[i]);
    }
  }
  return std::make_tuple(nullptr, 0);
}

uint64_t N4::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                         uint32_t &childrenCount) const {
  restart:
  bool needRestart = false;
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (uint32_t i = 0; i < count; ++i) {
    if (this->keys[i] >= start && this->keys[i] <= end) {
      children[childrenCount] = std::make_tuple(this->keys[i], this->children[i]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}