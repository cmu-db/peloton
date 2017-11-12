//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_node_16_children.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include <emmintrin.h>
#include "index/art_node.h"
#include "index/art_node_16_children.h"

namespace peloton {
namespace index {
bool N16::isFull() const {
  return count_ == 16;
}

bool N16::isUnderfull() const {
  return count_ == 3;
}

void N16::insert(uint8_t key, N *n) {
  uint8_t keyByteFlipped = flipSign(key);
  __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped), _mm_loadu_si128(reinterpret_cast<__m128i *>(keys)));
  uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - count_));
  unsigned pos = bitfield ? ctz(bitfield) : count_;
  memmove(keys + pos + 1, keys + pos, count_ - pos);
  memmove(children + pos + 1, children + pos, (count_ - pos) * sizeof(uintptr_t));
  keys[pos] = keyByteFlipped;
  children[pos] = n;
  count_++;
}

bool N16::Change(uint8_t key, N *val) {
  N **childPos = const_cast<N **>(GetChildPos(key));
  assert(childPos != nullptr);
  *childPos = val;
  return true;
}

bool N16::AddMultiValue(uint8_t key, uint64_t val) {
  N **childPos = const_cast<N **>(GetChildPos(key));
  assert(childPos != nullptr);
//  *childPos = val;
  TID tid = N::GetLeaf(*childPos);

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

N *const *N16::GetChildPos(const uint8_t k) const {
  __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                               _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
  unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count_) - 1);
  if (bitfield) {
    return &children[ctz(bitfield)];
  } else {
    return nullptr;
  }
}

N *N16::GetChild(const uint8_t k) const {
  N *const *childPos = GetChildPos(k);
  if (childPos == nullptr) {
    return nullptr;
  } else {
    return *childPos;
  }
}

void N16::remove(uint8_t k) {
  N *const *leafPlace = GetChildPos(k);
  assert(leafPlace != nullptr);
  std::size_t pos = leafPlace - children;
  memmove(keys + pos, keys + pos + 1, count_ - pos - 1);
  memmove(children + pos, children + pos + 1, (count_ - pos - 1) * sizeof(N *));
  count_--;
  assert(GetChild(k) == nullptr);
}

N *N16::GetAnyChild() const {
  for (int i = 0; i < count_; ++i) {
    if (N::IsLeaf(children[i])) {
      return children[i];
    }
  }
  return children[0];
}

void N16::DeleteChildren() {
  for (std::size_t i = 0; i < count_; ++i) {
    N::DeleteChildren(children[i]);
    N::DeleteNode(children[i]);
  }
}

uint64_t N16::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
  restart:
  bool needRestart = false;
  uint64_t v;
  v = ReadLockOrRestart(needRestart);
  if (needRestart) goto restart;
  childrenCount = 0;
  auto startPos = GetChildPos(start);
  auto endPos = GetChildPos(end);
  if (startPos == nullptr) {
    startPos = this->children;
  }
  if (endPos == nullptr) {
    endPos = this->children + (count_ - 1);
  }
  for (auto p = startPos; p <= endPos; ++p) {
    children[childrenCount] = std::make_tuple(flipSign(keys[p - this->children]), *p);
    childrenCount++;
  }
  ReadUnlockOrRestart(v, needRestart);
  if (needRestart) goto restart;
  return v;
}
}
}