//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_node_4_children.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include "index/art_node.h"
#include "index/art_node_4_children.h"

namespace peloton {
namespace index {
void N4::DeleteChildren() {
  for (uint32_t i = 0; i < count_; ++i) {
    N::DeleteChildren(children[i]);
    N::DeleteNode(children[i]);
  }
}

bool N4::isFull() const { return count_ == 4; }

bool N4::isUnderfull() const { return false; }

void N4::insert(uint8_t key, N *n) {
  unsigned pos;
  for (pos = 0; (pos < count_) && (keys[pos] < key); pos++)
    ;
  memmove(keys + pos + 1, keys + pos, count_ - pos);
  memmove(children + pos + 1, children + pos, (count_ - pos) * sizeof(N *));
  keys[pos] = key;
  children[pos] = n;
  count_++;
}

bool N4::Change(uint8_t key, N *val) {
  for (uint32_t i = 0; i < count_; ++i) {
    if (keys[i] == key) {
      children[i] = val;
      return true;
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N4::AddMultiValue(uint8_t key, uint64_t val) {
  for (uint32_t i = 0; i < count_; ++i) {
    if (keys[i] == key) {
      //      children[i] = val;
      TID tid = N::GetLeaf(children[i]);

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
  }
  assert(false);
  __builtin_unreachable();
}

N *N4::GetChild(const uint8_t k) const {
  for (uint32_t i = 0; i < count_; ++i) {
    if (keys[i] == k) {
      return children[i];
    }
  }
  return nullptr;
}

void N4::remove(uint8_t k, ThreadInfo &thread_info) {
  for (uint32_t i = 0; i < count_; ++i) {
    if (keys[i] == k) {
      if (N::IsLeaf(children[i])) {
        thread_info.GetEpochManager().MarkNodeForDeletion((MultiValues *)N::GetLeaf(children[i]), thread_info);
      }
      memmove(keys + i, keys + i + 1, count_ - i - 1);
      memmove(children + i, children + i + 1, (count_ - i - 1) * sizeof(N *));
      count_--;
      return;
    }
  }
}

N *N4::GetAnyChild() const {
  N *anyChild = nullptr;
  for (uint32_t i = 0; i < count_; ++i) {
    if (N::IsLeaf(children[i])) {
      return children[i];
    } else {
      anyChild = children[i];
    }
  }
  return anyChild;
}

std::tuple<N *, uint8_t> N4::GetSecondChild(const uint8_t key) const {
  for (uint32_t i = 0; i < count_; ++i) {
    if (keys[i] != key) {
      return std::make_tuple(children[i], keys[i]);
    }
  }
  return std::make_tuple(nullptr, 0);
}

uint64_t N4::GetChildren(uint8_t start, uint8_t end,
                         std::tuple<uint8_t, N *> *&children,
                         uint32_t &childrenCount) const {
restart:
  bool needRestart = false;
  uint64_t v;
  v = ReadLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (uint32_t i = 0; i < count_; ++i) {
    if (this->keys[i] >= start && this->keys[i] <= end) {
      children[childrenCount] =
          std::make_tuple(this->keys[i], this->children[i]);
      childrenCount++;
    }
  }
  ReadUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}