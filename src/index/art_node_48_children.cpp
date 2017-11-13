//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_node_48_children.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include "index/art_node.h"
#include "index/art_node_4_children.h"
#include "index/art_node_16_children.h"
#include "index/art_node_48_children.h"
#include "index/art_node_256_children.h"

namespace peloton {
namespace index {
bool N48::isFull() const { return count_ == 48; }

bool N48::isUnderfull() const { return count_ == 12; }

void N48::insert(uint8_t key, N *n) {
  unsigned pos = count_;
  if (children[pos]) {
    for (pos = 0; children[pos] != nullptr; pos++)
      ;
  }
  children[pos] = n;
  childIndex[key] = (uint8_t)pos;
  count_++;
}

bool N48::Change(uint8_t key, N *val) {
  children[childIndex[key]] = val;
  return true;
}

bool N48::AddMultiValue(uint8_t key, uint64_t val) {
  //  children[childIndex[key]] = val;
  TID tid = N::GetLeaf(children[childIndex[key]]);

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

N *N48::GetChild(const uint8_t k) const {
  if (childIndex[k] == emptyMarker) {
    return nullptr;
  } else {
    return children[childIndex[k]];
  }
}

void N48::remove(uint8_t k, ThreadInfo &thread_info) {
  assert(childIndex[k] != emptyMarker);
  if (N::IsLeaf(children[childIndex[k]])) {
    thread_info.GetEpochManager().MarkNodeForDeletion((MultiValues *)N::GetLeaf(children[childIndex[k]]), thread_info);
  }
  children[childIndex[k]] = nullptr;
  childIndex[k] = emptyMarker;
  count_--;
  assert(GetChild(k) == nullptr);
}

N *N48::GetAnyChild() const {
  N *anyChild = nullptr;
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      if (N::IsLeaf(children[childIndex[i]])) {
        return children[childIndex[i]];
      } else {
        anyChild = children[childIndex[i]];
      };
    }
  }
  return anyChild;
}

void N48::DeleteChildren() {
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      N::DeleteChildren(children[childIndex[i]]);
      N::DeleteNode(children[childIndex[i]]);
    }
  }
}

uint64_t N48::GetChildren(uint8_t start, uint8_t end,
                          std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
restart:
  bool needRestart = false;
  uint64_t v;
  v = ReadLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->childIndex[i] != emptyMarker) {
      children[childrenCount] =
          std::make_tuple(i, this->children[this->childIndex[i]]);
      childrenCount++;
    }
  }
  ReadUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}