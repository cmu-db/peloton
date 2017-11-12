//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_node.cpp
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
void N::SetType(NTypes type) {
  type_version_lock_obsolete_.fetch_add(ConvertTypeToVersion(type));
}

uint64_t N::ConvertTypeToVersion(NTypes type) {
  return (static_cast<uint64_t>(type) << 62);
}

NTypes N::GetType() const {
  return static_cast<NTypes>(type_version_lock_obsolete_.load(std::memory_order_relaxed) >> 62);
}

void N::WriteLockOrRestart(bool &needRestart) {

  uint64_t version;
  version = ReadLockOrRestart(needRestart);
  if (needRestart) return;

  UpgradeToWriteLockOrRestart(version, needRestart);
  if (needRestart) return;
}

void N::UpgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
  if (type_version_lock_obsolete_.compare_exchange_strong(version, version + 0b10)) {
    version = version + 0b10;
  } else {
    needRestart = true;
  }
}

void N::WriteUnlock() {
  type_version_lock_obsolete_.fetch_add(0b10);
}

N *N::GetAnyChild(const N *node) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->GetAnyChild();
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->GetAnyChild();
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->GetAnyChild();
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->GetAnyChild();
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N::Change(N *node, uint8_t key, N *val) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->Change(key, val);
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      return n->Change(key, val);
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      return n->Change(key, val);
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      return n->Change(key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N::AddMultiValue(N *node, uint8_t key, uint64_t val) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->AddMultiValue(key, val);
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      return n->AddMultiValue(key, val);
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      return n->AddMultiValue(key, val);
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      return n->AddMultiValue(key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

template<typename curN, typename biggerN>
void N::InsertGrow(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart, ThreadInfo &threadInfo) {
  if (!n->isFull()) {
    if (parentNode != nullptr) {
      parentNode->ReadUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    n->UpgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) return;
    n->insert(key, val);
    n->WriteUnlock();
    return;
  }

  parentNode->UpgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) return;

  n->UpgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) {
    parentNode->WriteUnlock();
    return;
  }

  auto nBig = new biggerN(n->GetPrefix(), n->GetPrefixLength());
  n->copyTo(nBig);
  nBig->insert(key, val);

  N::Change(parentNode, keyParent, nBig);

  n->WriteUnlockObsolete();
  threadInfo.GetEpochManager().MarkNodeForDeletion(n, threadInfo);
  parentNode->WriteUnlock();
}

void N::InsertAndUnlock(N *node, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart, ThreadInfo &threadInfo) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      InsertGrow<N4, N16>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart, threadInfo);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      InsertGrow<N16, N48>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart, threadInfo);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      InsertGrow<N48, N256>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart, threadInfo);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      InsertGrow<N256, N256>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart, threadInfo);
      break;
    }
  }
}

N *N::GetChild(const uint8_t k, const N *node) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->GetChild(k);
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->GetChild(k);
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->GetChild(k);
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->GetChild(k);
    }
  }
  assert(false);
  __builtin_unreachable();
}

void N::DeleteChildren(N *node) {
  if (N::IsLeaf(node)) {
    return;
  }
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      n->DeleteChildren();
      return;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      n->DeleteChildren();
      return;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      n->DeleteChildren();
      return;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      n->DeleteChildren();
      return;
    }
  }
  assert(false);
  __builtin_unreachable();
}

template<typename curN, typename smallerN>
void N::RemoveAndShrink(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, bool &needRestart, ThreadInfo &threadInfo) {
  if (!n->isUnderfull() || parentNode == nullptr) {
    if (parentNode != nullptr) {
      parentNode->ReadUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    n->UpgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) return;

    n->remove(key);
    n->WriteUnlock();
    return;
  }
  parentNode->UpgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) return;

  n->UpgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) {
    parentNode->WriteUnlock();
    return;
  }

  auto nSmall = new smallerN(n->GetPrefix(), n->GetPrefixLength());

  n->copyTo(nSmall);
  nSmall->remove(key);
  N::Change(parentNode, keyParent, nSmall);

  n->WriteUnlockObsolete();
  threadInfo.GetEpochManager().MarkNodeForDeletion(n, threadInfo);
  parentNode->WriteUnlock();
}

void N::RemoveAndUnlock(N *node, uint64_t v, uint8_t key, N *parentNode, uint64_t parentVersion, uint8_t keyParent, bool &needRestart, ThreadInfo &threadInfo) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      RemoveAndShrink<N4, N4>(n, v, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      RemoveAndShrink<N16, N4>(n, v, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      RemoveAndShrink<N48, N16>(n, v, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      RemoveAndShrink<N256, N48>(n, v, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
  }
}

void N::RemoveLockedNodeAndUnlock(N *node, uint8_t key, N *parentNode, uint64_t parentVersion, uint8_t keyParent, bool &needRestart, ThreadInfo &threadInfo) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      RemoveLockedNodeAndShrink<N4, N4>(n, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      RemoveLockedNodeAndShrink<N16, N4>(n, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      RemoveLockedNodeAndShrink<N48, N16>(n, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      RemoveLockedNodeAndShrink<N256, N48>(n, parentNode, parentVersion, keyParent, key, needRestart, threadInfo);
      break;
    }
  }
}

template<typename curN, typename smallerN>
void N::RemoveLockedNodeAndShrink(curN *n, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, bool &needRestart, ThreadInfo &threadInfo) {
  if (!n->isUnderfull() || parentNode == nullptr) {
    if (parentNode != nullptr) {
      parentNode->ReadUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }

    n->remove(key);
    n->WriteUnlock();
    return;
  }
  parentNode->UpgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) {
    n->WriteUnlock();
    return;
  }

  auto nSmall = new smallerN(n->GetPrefix(), n->GetPrefixLength());

  n->copyTo(nSmall);
  nSmall->remove(key);
  N::Change(parentNode, keyParent, nSmall);

  n->WriteUnlockObsolete();
  threadInfo.GetEpochManager().MarkNodeForDeletion(n, threadInfo);
  parentNode->WriteUnlock();
}

bool N::IsLocked(uint64_t version) const {
  return ((version & 0b10) == 0b10);
}

uint64_t N::ReadLockOrRestart(bool &needRestart) const {
  uint64_t version;
  version = type_version_lock_obsolete_.load();
/*        do {
            version = type_version_lock_obsolete_.load();
        } while (IsLocked(version));*/
  if (IsLocked(version) || IsObsolete(version)) {
    needRestart = true;
  }
  return version;
  //uint64_t version;
  //while (IsLocked(version)) _mm_pause();
  //return version;
}

bool N::IsObsolete(uint64_t version) {
  return (version & 1) == 1;
}

void N::CheckOrRestart(uint64_t startRead, bool &needRestart) const {
  ReadUnlockOrRestart(startRead, needRestart);
}

void N::ReadUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
  needRestart = (startRead != type_version_lock_obsolete_.load());
}

uint32_t N::GetPrefixLength() const {
  return prefix_count_;
}

bool N::HasPrefix() const {
  return prefix_count_ > 0;
}

uint32_t N::GetCount() const {
  return count_;
}

const uint8_t *N::GetPrefix() const {
  return prefix_;
}

void N::SetPrefix(const uint8_t *prefix, uint32_t length) {
  if (length > 0) {
    memcpy(this->prefix_, prefix, std::min(length, max_stored_prefix_length));
    prefix_count_ = length;
  } else {
    prefix_count_ = 0;
  }
}

void N::AddPrefixBefore(N *node, uint8_t key) {
  uint32_t prefixCopyCount = std::min(max_stored_prefix_length, node->GetPrefixLength() + 1);
  memmove(this->prefix_ + prefixCopyCount, this->prefix_,
          std::min(this->GetPrefixLength(), max_stored_prefix_length - prefixCopyCount));
  memcpy(this->prefix_, node->prefix_, std::min(prefixCopyCount, node->GetPrefixLength()));
  if (node->GetPrefixLength() < max_stored_prefix_length) {
    this->prefix_[prefixCopyCount - 1] = key;
  }
  this->prefix_count_ += node->GetPrefixLength() + 1;
}


bool N::IsLeaf(const N *n) {
  return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) == (static_cast<uint64_t>(1) << 63);
}

N *N::SetLeaf(TID tid) {
  MultiValues *value_list = new MultiValues();
  value_list->tid = tid;
  value_list->next = 0;
//  return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63));
  return reinterpret_cast<N *>((uint64_t)(value_list) | (static_cast<uint64_t>(1) << 63));
}

N *N::SetLeafWithListPointer(MultiValues *value_list) {
//  return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63));
  return reinterpret_cast<N *>((uint64_t)(value_list) | (static_cast<uint64_t>(1) << 63));
}

TID N::GetLeaf(const N *n) {
  return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
}

std::tuple<N *, uint8_t> N::GetSecondChild(N *node, const uint8_t key) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->GetSecondChild(key);
    }
    default: {
      assert(false);
      __builtin_unreachable();
    }
  }
}

void N::DeleteNode(N *node) {
  if (N::IsLeaf(node)) {
    return;
  }
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      delete n;
      return;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      delete n;
      return;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      delete n;
      return;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      delete n;
      return;
    }
  }
  delete node;
}


TID N::GetAnyChildTid(const N *n, bool &needRestart) {
  const N *nextNode = n;

  while (true) {
    const N *node = nextNode;
    auto v = node->ReadLockOrRestart(needRestart);
    if (needRestart) return 0;

    nextNode = GetAnyChild(node);
    node->ReadUnlockOrRestart(v, needRestart);
    if (needRestart) return 0;

    assert(nextNode != nullptr);
    if (IsLeaf(nextNode)) {
      return GetLeaf(nextNode);
    }
  }
}

uint64_t N::GetChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                        uint32_t &childrenCount) {
  switch (node->GetType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->GetChildren(start, end, children, childrenCount);
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->GetChildren(start, end, children, childrenCount);
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->GetChildren(start, end, children, childrenCount);
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->GetChildren(start, end, children, childrenCount);
    }
  }
  assert(false);
  __builtin_unreachable();
}
}
}