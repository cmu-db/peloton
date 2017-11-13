//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <algorithm>
#include <emmintrin.h>
#include <functional>
#include "index/art.h"
#include "index/art_key.h"
#include "index/art_epoch_manager.h"
#include "index/art_node.h"
#include "index/art_node_4_children.h"
#include "index/art_node_16_children.h"
#include "index/art_node_48_children.h"
#include "index/art_node_256_children.h"
#include "index/index.h"

namespace peloton {
namespace index {

AdaptiveRadixTree::AdaptiveRadixTree(LoadKeyFunction loadKey)
    : root_(new N256(nullptr, 0)), load_key_(loadKey), epoch_manager_(256) {}

AdaptiveRadixTree::~AdaptiveRadixTree() {
  N::DeleteChildren(root_);
  N::DeleteNode(root_);
}

ThreadInfo &AdaptiveRadixTree::GetThreadInfo() {
  static thread_local int gc_id = (this->epoch_manager_).thread_info_counter_++;
  return (this->epoch_manager_).getThreadInfoByID(gc_id);
}

void AdaptiveRadixTree::SetIndexMetadata(IndexMetadata *metadata) {
  this->metadata_ = metadata;
}

void yield(int count) {
  if (count > 3)
    sched_yield();
  else
    _mm_pause();
}

TID AdaptiveRadixTree::Lookup(const ARTKey &k, ThreadInfo &thread_epoch_info,
                              std::vector<ItemPointer *> &result) const {
  EpochGuardReadonly epoche_guard(thread_epoch_info);
  int restart_count = 0;
restart:
  if (restart_count++) yield(restart_count);
  bool need_restart = false;

  N *node;
  N *parent_node = nullptr;
  uint64_t v;
  uint32_t level = 0;
  bool optimistic_prefix_match = false;

  node = root_;
  v = node->ReadLockOrRestart(need_restart);
  if (need_restart) goto restart;
  while (true) {
    switch (CheckPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        node->ReadUnlockOrRestart(v, need_restart);
        if (need_restart) goto restart;
        return 0;
      case CheckPrefixResult::OptimisticMatch:
        optimistic_prefix_match = true;
      // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          return 0;
        }
        parent_node = node;
        node = N::GetChild(k[level], parent_node);
        parent_node->CheckOrRestart(v, need_restart);
        if (need_restart) goto restart;

        if (node == nullptr) {
          return 0;
        }
        if (N::IsLeaf(node)) {
          parent_node->ReadUnlockOrRestart(v, need_restart);
          if (need_restart) goto restart;

          TID tid = N::GetLeaf(node);
          if (level < k.getKeyLen() - 1 || optimistic_prefix_match) {
            if (CheckKey(tid, k) == 0) {
              return 0;
            }
          }
          MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
          while (value_list != nullptr) {
            ItemPointer *value_pointer = (ItemPointer *)(value_list->tid);
            result.push_back(value_pointer);
            value_list = (MultiValues *)value_list->next.load();
          }
          return tid;
        }
        level++;
    }
    uint64_t nv = node->ReadLockOrRestart(need_restart);
    if (need_restart) goto restart;

    parent_node->ReadUnlockOrRestart(v, need_restart);
    if (need_restart) goto restart;
    v = nv;
  }
}

bool AdaptiveRadixTree::LookupRange(const ARTKey &start, const ARTKey &end,
                                    ARTKey &continue_key,
                                    std::vector<ItemPointer *> &result,
                                    std::size_t result_size,
                                    std::size_t &results_found,
                                    ThreadInfo &thread_epoch_info) const {
  for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
    if (start[i] > end[i]) {
      results_found = 0;
      return false;
    } else if (start[i] < end[i]) {
      break;
    }
  }
  EpochGuard epoche_guard(thread_epoch_info);
  TID to_continue = 0;
  std::function<void(const N *)> Copy =
      [&result, &result_size, &results_found, &to_continue, &Copy](
          const N *node) {
        if (N::IsLeaf(node)) {
          //      if (results_found == result_size) {
          //        to_continue = N::GetLeaf(node);
          //        return;
          //      }
          //      result[results_found] = N::GetLeaf(node);

          TID tid = N::GetLeaf(node);
          MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
          while (value_list != nullptr) {
            ItemPointer *new_value = (ItemPointer *)(value_list->tid);
            result.push_back(new_value);
            results_found++;
            value_list = (MultiValues *)value_list->next.load();
          }
        } else {
          std::tuple<uint8_t, N *> children[256];
          uint32_t children_count = 0;
          N::GetChildren(node, 0u, 255u, children, children_count);
          for (uint32_t i = 0; i < children_count; ++i) {
            const N *n = std::get<1>(children[i]);
            Copy(n);
            if (to_continue != 0) {
              break;
            }
          }
        }
      };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> FindStart =
      [&Copy, &start, &FindStart, &to_continue, this](
          N *node, uint8_t nodeK, uint32_t level, const N *parent_node,
          uint64_t vp) {
        if (N::IsLeaf(node)) {
          Copy(node);
          return;
        }
        uint64_t v;
        PCCompareResults prefix_result;

        {
        read_again:
          bool need_restart = false;
          v = node->ReadLockOrRestart(need_restart);
          if (need_restart) goto read_again;

          prefix_result = CheckPrefixCompare(node, start, 0, level, load_key_,
                                             need_restart, metadata_);
          if (need_restart) goto read_again;

          parent_node->ReadUnlockOrRestart(vp, need_restart);
          if (need_restart) {
          read_parent_again:
            need_restart = false;
            vp = parent_node->ReadLockOrRestart(need_restart);
            if (need_restart) goto read_parent_again;

            node = N::GetChild(nodeK, parent_node);

            parent_node->ReadUnlockOrRestart(vp, need_restart);
            if (need_restart) goto read_parent_again;

            if (node == nullptr) {
              return;
            }
            if (N::IsLeaf(node)) {
              Copy(node);
              return;
            }
            goto read_again;
          }
          node->ReadUnlockOrRestart(v, need_restart);
          if (need_restart) goto read_again;
        }

        switch (prefix_result) {
          case PCCompareResults::Bigger:
            Copy(node);
            break;
          case PCCompareResults::Equal: {
            uint8_t start_level =
                (start.getKeyLen() > level) ? start[level] : 0;
            std::tuple<uint8_t, N *> children[256];
            uint32_t children_count = 0;
            v = N::GetChildren(node, start_level, 255, children,
                               children_count);
            for (uint32_t i = 0; i < children_count; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              N *n = std::get<1>(children[i]);
              if (k == start_level) {
                FindStart(n, k, level + 1, node, v);
              } else if (k > start_level) {
                Copy(n);
              }
              if (to_continue != 0) {
                break;
              }
            }
            break;
          }
          case PCCompareResults::Smaller:
            break;
        }
      };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> FindEnd =
      [&Copy, &end, &to_continue, &FindEnd, this](
          N *node, uint8_t nodeK, uint32_t level, const N *parent_node,
          uint64_t vp) {
        if (N::IsLeaf(node)) {
          // end boundary inclusive
          Copy(node);
          return;
        }
        uint64_t v;
        PCCompareResults prefix_result;
        {
        read_again:
          bool need_restart = false;
          v = node->ReadLockOrRestart(need_restart);
          if (need_restart) goto read_again;

          prefix_result = CheckPrefixCompare(node, end, 255, level, load_key_,
                                             need_restart, metadata_);
          if (need_restart) goto read_again;

          parent_node->ReadUnlockOrRestart(vp, need_restart);
          if (need_restart) {
          read_parent_again:
            vp = parent_node->ReadLockOrRestart(need_restart);
            if (need_restart) goto read_parent_again;

            node = N::GetChild(nodeK, parent_node);

            parent_node->ReadUnlockOrRestart(vp, need_restart);
            if (need_restart) goto read_parent_again;

            if (node == nullptr) {
              return;
            }
            if (N::IsLeaf(node)) {
              return;
            }
            goto read_again;
          }
          node->ReadUnlockOrRestart(v, need_restart);
          if (need_restart) goto read_again;
        }
        switch (prefix_result) {
          case PCCompareResults::Smaller:
            Copy(node);
            break;
          case PCCompareResults::Equal: {
            uint8_t end_level = (end.getKeyLen() > level) ? end[level] : 255;
            std::tuple<uint8_t, N *> children[256];
            uint32_t children_count = 0;
            v = N::GetChildren(node, 0, end_level, children, children_count);
            for (uint32_t i = 0; i < children_count; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              N *n = std::get<1>(children[i]);
              if (k == end_level) {
                FindEnd(n, k, level + 1, node, v);
              } else if (k < end_level) {
                Copy(n);
              }
              if (to_continue != 0) {
                break;
              }
            }
            break;
          }
          case PCCompareResults::Bigger:
            break;
        }
      };

  int restart_count = 0;
restart:
  if (restart_count++) yield(restart_count);
  bool need_restart = false;

  results_found = 0;

  uint32_t level = 0;
  N *node = nullptr;
  N *next_node = root_;
  N *parent_node;
  uint64_t v = 0;
  uint64_t vp;

  while (true) {
    parent_node = node;
    vp = v;
    node = next_node;
    PCEqualsResults prefix_result;
    v = node->ReadLockOrRestart(need_restart);
    if (need_restart) goto restart;
    prefix_result = CheckPrefixEquals(node, level, start, end, load_key_,
                                      need_restart, metadata_);
    if (need_restart) goto restart;
    if (parent_node != nullptr) {
      parent_node->ReadUnlockOrRestart(vp, need_restart);
      if (need_restart) goto restart;
    }
    node->ReadUnlockOrRestart(v, need_restart);
    if (need_restart) goto restart;

    switch (prefix_result) {
      case PCEqualsResults::NoMatch: {
        return false;
      }
      case PCEqualsResults::Contained: {
        Copy(node);
        break;
      }
      case PCEqualsResults::BothMatch: {
        uint8_t start_level = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t end_level = (end.getKeyLen() > level) ? end[level] : 255;
        if (start_level != end_level) {
          std::tuple<uint8_t, N *> children[256];
          uint32_t children_count = 0;
          v = N::GetChildren(node, start_level, end_level, children,
                             children_count);
          for (uint32_t i = 0; i < children_count; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            N *n = std::get<1>(children[i]);
            if (k == start_level) {
              FindStart(n, k, level + 1, node, v);
            } else if (k > start_level && k < end_level) {
              Copy(n);
            } else if (k == end_level) {
              FindEnd(n, k, level + 1, node, v);
            }
            if (to_continue) {
              break;
            }
          }
        } else {
          next_node = N::GetChild(start_level, node);
          node->ReadUnlockOrRestart(v, need_restart);
          if (need_restart) goto restart;
          level++;
          continue;
        }
        break;
      }
    }
    break;
  }
  if (to_continue != 0) {
    load_key_(to_continue, continue_key, metadata_);
    return true;
  } else {
    return false;
  }
}

void AdaptiveRadixTree::ScanAllLeafNodes(const N *node,
                                         std::vector<ItemPointer *> &result,
                                         std::size_t &result_count) const {
  if (N::IsLeaf(node)) {
    TID tid = N::GetLeaf(node);
    MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
    while (value_list != nullptr) {
      ItemPointer *new_value = (ItemPointer *)(value_list->tid);
      result.push_back(new_value);
      result_count++;
      value_list = (MultiValues *)value_list->next.load();
    }
  } else {
    std::tuple<uint8_t, N *> children[256];
    uint32_t children_count = 0;
    N::GetChildren(node, 0u, 255u, children, children_count);
    for (uint32_t i = 0; i < children_count; ++i) {
      const N *n = std::get<1>(children[i]);
      ScanAllLeafNodes(n, result, result_count);
    }
  }
}

void AdaptiveRadixTree::FullScan(std::vector<ItemPointer *> &result,
                                 std::size_t &result_count,
                                 ThreadInfo &thread_epoch_info) const {
  EpochGuard epoche_guard(thread_epoch_info);
  ScanAllLeafNodes(root_, result, result_count);
}

TID AdaptiveRadixTree::CheckKey(const TID tid, const ARTKey &k) const {
  ARTKey kt;
  this->load_key_(tid, kt, metadata_);
  if (k == kt) {
    return tid;
  }
  return 0;
}

void AdaptiveRadixTree::Insert(const ARTKey &k, TID tid,
                               ThreadInfo &thread_epoch_info,
                               bool &insert_success) {
  EpochGuard epoche_guard(thread_epoch_info);
  int restart_count = 0;
  insert_success = false;
restart:
  if (restart_count++) yield(restart_count);
  bool need_restart = false;

  N *node = nullptr;
  N *next_node = root_;
  N *parent_node = nullptr;
  uint8_t parent_key, node_key = 0;
  uint64_t parent_version = 0;
  uint32_t level = 0;

  while (true) {
    parent_node = node;
    parent_key = node_key;
    node = next_node;
    auto v = node->ReadLockOrRestart(need_restart);
    if (need_restart) goto restart;

    uint32_t next_level = level;

    uint8_t non_matching_key;
    Prefix remaining_prefix;
    auto res = CheckPrefixPessimistic(
        node, k, next_level, non_matching_key, remaining_prefix,
        this->load_key_, need_restart, metadata_);  // increases level
    if (need_restart) goto restart;
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        parent_node->UpgradeToWriteLockOrRestart(parent_version, need_restart);
        if (need_restart) goto restart;

        node->UpgradeToWriteLockOrRestart(v, need_restart);
        if (need_restart) {
          parent_node->WriteUnlock();
          goto restart;
        }
        // 1) Create new node which will be parent of node, Set common prefix,
        // level to this node
        auto newNode = new N4(node->GetPrefix(), next_level - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[next_level], N::SetLeaf(tid));
        newNode->insert(non_matching_key, node);

        // 3) UpgradeToWriteLockOrRestart, update parent_node to point to the
        // new node, unlock
        N::Change(parent_node, parent_key, newNode);
        parent_node->WriteUnlock();

        // 4) update prefix of node, unlock
        node->SetPrefix(remaining_prefix,
                        node->GetPrefixLength() - ((next_level - level) + 1));

        node->WriteUnlock();
        insert_success = true;
        return;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = next_level;
    node_key = k[level];
    next_node = N::GetChild(node_key, node);
    node->CheckOrRestart(v, need_restart);
    if (need_restart) goto restart;

    if (next_node == nullptr) {
      N::InsertAndUnlock(node, v, parent_node, parent_version, parent_key,
                         node_key, N::SetLeaf(tid), need_restart,
                         thread_epoch_info);
      if (need_restart) goto restart;
      insert_success = true;
      return;
    }

    if (parent_node != nullptr) {
      parent_node->ReadUnlockOrRestart(parent_version, need_restart);
      if (need_restart) goto restart;
    }

    if (N::IsLeaf(next_node)) {
      node->UpgradeToWriteLockOrRestart(v, need_restart);
      if (need_restart) goto restart;

      ARTKey key;
      load_key_(N::GetLeaf(next_node), key, metadata_);

      if (key == k) {
        N::AddMultiValue(node, k[level], tid);
        node->WriteUnlock();
        insert_success = true;
        return;
      }

      level++;
      uint32_t prefix_length = 0;
      while (key[level + prefix_length] == k[level + prefix_length]) {
        prefix_length++;
      }

      auto n4 = new N4(&k[level], prefix_length);
      n4->insert(k[level + prefix_length], N::SetLeaf(tid));
      n4->insert(key[level + prefix_length], next_node);
      N::Change(node, k[level - 1], n4);
      node->WriteUnlock();
      insert_success = true;
      return;
    }
    level++;
    parent_version = v;
  }
}

bool AdaptiveRadixTree::ConditionalInsert(
    const ARTKey &k, TID tid, ThreadInfo &thread_epoch_info,
    std::function<bool(const void *)> predicate) {
  EpochGuard epoche_guard(thread_epoch_info);
  int restart_count = 0;
restart:
  if (restart_count++) yield(restart_count);
  bool need_restart = false;

  N *node = nullptr;
  N *next_node = root_;
  N *parent_node = nullptr;
  uint8_t parent_key, node_key = 0;
  uint64_t parent_version = 0;
  uint32_t level = 0;

  while (true) {
    parent_node = node;
    parent_key = node_key;
    node = next_node;
    auto v = node->ReadLockOrRestart(need_restart);
    if (need_restart) goto restart;

    uint32_t next_level = level;

    uint8_t non_matching_key;
    Prefix remaining_prefix;
    auto res = CheckPrefixPessimistic(
        node, k, next_level, non_matching_key, remaining_prefix,
        this->load_key_, need_restart, metadata_);  // increases level
    if (need_restart) goto restart;
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        parent_node->UpgradeToWriteLockOrRestart(parent_version, need_restart);
        if (need_restart) goto restart;

        node->UpgradeToWriteLockOrRestart(v, need_restart);
        if (need_restart) {
          parent_node->WriteUnlock();
          goto restart;
        }
        // 1) Create new node which will be parent of node, Set common prefix,
        // level to this node
        auto newNode = new N4(node->GetPrefix(), next_level - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[next_level], N::SetLeaf(tid));
        newNode->insert(non_matching_key, node);

        // 3) UpgradeToWriteLockOrRestart, update parent_node to point to the
        // new node, unlock
        N::Change(parent_node, parent_key, newNode);
        parent_node->WriteUnlock();

        // 4) update prefix of node, unlock
        node->SetPrefix(remaining_prefix,
                        node->GetPrefixLength() - ((next_level - level) + 1));

        node->WriteUnlock();
        return true;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = next_level;
    node_key = k[level];
    next_node = N::GetChild(node_key, node);
    node->CheckOrRestart(v, need_restart);
    if (need_restart) goto restart;

    if (next_node == nullptr) {
      N::InsertAndUnlock(node, v, parent_node, parent_version, parent_key,
                         node_key, N::SetLeaf(tid), need_restart,
                         thread_epoch_info);
      if (need_restart) goto restart;
      return true;
    }

    if (parent_node != nullptr) {
      parent_node->ReadUnlockOrRestart(parent_version, need_restart);
      if (need_restart) goto restart;
    }

    if (N::IsLeaf(next_node)) {
      node->UpgradeToWriteLockOrRestart(v, need_restart);
      if (need_restart) goto restart;

      ARTKey key;
      load_key_(N::GetLeaf(next_node), key, metadata_);

      if (key == k) {
        MultiValues *value_list =
            reinterpret_cast<MultiValues *>(N::GetLeaf(next_node));
        while (value_list != nullptr) {
          ItemPointer *value_pointer = (ItemPointer *)(value_list->tid);
          if (predicate != nullptr && predicate(value_pointer)) {
            node->WriteUnlock();
            return false;
          } else if ((uint64_t)value_pointer == tid) {
            node->WriteUnlock();
            return false;
          }
          value_list = (MultiValues *)value_list->next.load();
        }

        N::AddMultiValue(node, k[level], tid);
        node->WriteUnlock();
        return true;
      }

      level++;
      uint32_t prefix_length = 0;
      while (key[level + prefix_length] == k[level + prefix_length]) {
        prefix_length++;
      }

      auto n4 = new N4(&k[level], prefix_length);
      n4->insert(k[level + prefix_length], N::SetLeaf(tid));
      n4->insert(key[level + prefix_length], next_node);
      N::Change(node, k[level - 1], n4);
      node->WriteUnlock();
      return true;
    }
    level++;
    parent_version = v;
  }
}

bool AdaptiveRadixTree::Remove(const ARTKey &k, TID tid,
                               ThreadInfo &thread_epoch_info) {
  EpochGuard epoche_guard(thread_epoch_info);
  int restart_count = 0;
restart:
  if (restart_count++) yield(restart_count);
  bool need_restart = false;

  N *node = nullptr;
  N *next_node = root_;
  N *parent_node = nullptr;
  uint8_t parent_key, node_key = 0;
  uint64_t parent_version = 0;
  uint32_t level = 0;

  while (true) {
    parent_node = node;
    parent_key = node_key;
    node = next_node;
    auto v = node->ReadLockOrRestart(need_restart);
    if (need_restart) goto restart;

    switch (CheckPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        node->ReadUnlockOrRestart(v, need_restart);
        if (need_restart) goto restart;
        return false;
      case CheckPrefixResult::OptimisticMatch:
      // fallthrough
      case CheckPrefixResult::Match: {
        node_key = k[level];
        next_node = N::GetChild(node_key, node);

        node->CheckOrRestart(v, need_restart);
        if (need_restart) goto restart;

        if (next_node == nullptr) {
          node->ReadUnlockOrRestart(v, need_restart);
          if (need_restart) goto restart;
          return false;
        }
        if (N::IsLeaf(next_node)) {
          node->UpgradeToWriteLockOrRestart(v, need_restart);
          if (need_restart) goto restart;

          MultiValues *parent_value = nullptr;
          MultiValues *value_list =
              reinterpret_cast<MultiValues *>(N::GetLeaf(next_node));
          uint32_t value_count = 0;
          bool value_found = false;
          while (value_list != nullptr) {
            value_count++;
            if (value_list->tid == tid) {
              value_found = true;
              if (value_list->next != 0) {
                value_count++;
              }
              break;
            }
            parent_value = value_list;
            value_list = (MultiValues *)value_list->next.load();
          }

          if (!value_found) {
            node->WriteUnlock();
            return false;
          }

          if (value_count > 1) {
            if (parent_value != nullptr) {
              uint64_t expected = parent_value->next;
              uint64_t new_next = value_list->next;
              parent_value->next.compare_exchange_strong(expected, new_next);
              this->epoch_manager_.MarkNodeForDeletion(value_list,
                                                       thread_epoch_info);
            } else {
              N::Change(node, k[level],
                        N::SetLeafWithListPointer(
                            (MultiValues *)value_list->next.load()));
              this->epoch_manager_.MarkNodeForDeletion(value_list,
                                                       thread_epoch_info);
            }

            // remember to unlock the node!!
            node->WriteUnlock();
          } else {
            // last value in the value_list is deleted
            assert(parent_node == nullptr || node->GetCount() != 1);
            if (node->GetCount() == 2 && parent_node != nullptr) {
              parent_node->UpgradeToWriteLockOrRestart(parent_version,
                                                       need_restart);
              if (need_restart) {
                node->WriteUnlock();
                goto restart;
              }

              // 1. check remaining entries
              N *secondNodeN;
              uint8_t secondNodeK;
              std::tie(secondNodeN, secondNodeK) =
                  N::GetSecondChild(node, node_key);
              if (N::IsLeaf(secondNodeN)) {
                // N::remove(node, k[level]); not necessary
                N::Change(parent_node, parent_key, secondNodeN);

                parent_node->WriteUnlock();
                node->WriteUnlockObsolete();
                this->epoch_manager_.MarkNodeForDeletion(node,
                                                         thread_epoch_info);
              } else {
                secondNodeN->WriteLockOrRestart(need_restart);
                if (need_restart) {
                  node->WriteUnlock();
                  parent_node->WriteUnlock();
                  goto restart;
                }

                // N::remove(node, k[level]); not necessary
                N::Change(parent_node, parent_key, secondNodeN);
                parent_node->WriteUnlock();

                secondNodeN->AddPrefixBefore(node, secondNodeK);
                secondNodeN->WriteUnlock();

                node->WriteUnlockObsolete();
                this->epoch_manager_.MarkNodeForDeletion(node,
                                                         thread_epoch_info);
              }
            } else {
              N::RemoveLockedNodeAndUnlock(node, k[level], parent_node,
                                           parent_version, parent_key,
                                           need_restart, thread_epoch_info);
              if (need_restart) goto restart;
            }
          }

          return true;
        }
        level++;
        parent_version = v;
      }
    }
  }
}

inline typename AdaptiveRadixTree::CheckPrefixResult
AdaptiveRadixTree::CheckPrefix(N *n, const ARTKey &k, uint32_t &level) {
  if (n->HasPrefix()) {
    if (k.getKeyLen() <= level + n->GetPrefixLength()) {
      return CheckPrefixResult::NoMatch;
    }
    for (uint32_t i = 0;
         i < std::min(n->GetPrefixLength(), max_stored_prefix_length); ++i) {
      if (n->GetPrefix()[i] != k[level]) {
        return CheckPrefixResult::NoMatch;
      }
      ++level;
    }
    if (n->GetPrefixLength() > max_stored_prefix_length) {
      level = level + (n->GetPrefixLength() - max_stored_prefix_length);
      return CheckPrefixResult::OptimisticMatch;
    }
  }
  return CheckPrefixResult::Match;
}

typename AdaptiveRadixTree::CheckPrefixPessimisticResult
AdaptiveRadixTree::CheckPrefixPessimistic(
    N *n, const ARTKey &k, uint32_t &level, uint8_t &non_matching_key,
    Prefix &non_matching_prefix, LoadKeyFunction loadKey, bool &need_restart,
    IndexMetadata *metadata) {
  if (n->HasPrefix()) {
    uint32_t prevLevel = level;
    ARTKey kt;
    for (uint32_t i = 0; i < n->GetPrefixLength(); ++i) {
      if (i == max_stored_prefix_length) {
        /*
         * ART uses path compression, assume that the pessimistic path
         * compression only records 3 bytes of prefix in each node, consider
         * a case where only keys are inserted in the ART, key 'abcdef123'
         * and key 'abcdef456', 3 nodes will be constructed, a root node
         * which records a prefix 'abc' ('def' is skipped due to optimistic
         * path compression) and has two pointers, pointer '1' to left leaf
         * node and pointer '4' to right leaf node; 2 leaf nodes only store
         * the pointer to the tuple; when doing a lookup for key 'abcdef789'
         * or key 'abcxyzrst', ART knows the first 3 bytes match the prefix
         * recorded in root node and it also knows 3 bytes are missing in
         * the actual prefix that root node represents, so ART uses the value
         * in any leaf values below the current node to reconstruct a key and
         * continues comparing the remaining 3 bytes missing because of
         * optimistic path compression. The idea of getting any children is
         * smart because any keys that go through this node share the same
         * 6 bytes.
         */
        auto any_tid = N::GetAnyChildTid(n, need_restart);
        if (need_restart) return CheckPrefixPessimisticResult::Match;
        loadKey(any_tid, kt, metadata);
      }
      uint8_t current_byte =
          i >= max_stored_prefix_length ? kt[level] : n->GetPrefix()[i];
      if (current_byte != k[level]) {
        non_matching_key = current_byte;
        if (n->GetPrefixLength() > max_stored_prefix_length) {
          if (i < max_stored_prefix_length) {
            auto any_tid = N::GetAnyChildTid(n, need_restart);
            if (need_restart) return CheckPrefixPessimisticResult::Match;
            loadKey(any_tid, kt, metadata);
          }
          memcpy(non_matching_prefix, &kt[0] + level + 1,
                 std::min((n->GetPrefixLength() - (level - prevLevel) - 1),
                          max_stored_prefix_length));
        } else {
          memcpy(non_matching_prefix, n->GetPrefix() + i + 1,
                 n->GetPrefixLength() - i - 1);
        }
        return CheckPrefixPessimisticResult::NoMatch;
      }
      ++level;
    }
  }
  return CheckPrefixPessimisticResult::Match;
}

typename AdaptiveRadixTree::PCCompareResults
AdaptiveRadixTree::CheckPrefixCompare(const N *n, const ARTKey &k,
                                      uint8_t fillKey, uint32_t &level,
                                      LoadKeyFunction loadKey,
                                      bool &need_restart,
                                      IndexMetadata *metadata) {
  if (n->HasPrefix()) {
    ARTKey kt;
    for (uint32_t i = 0; i < n->GetPrefixLength(); ++i) {
      if (i == max_stored_prefix_length) {
        auto any_tid = N::GetAnyChildTid(n, need_restart);
        if (need_restart) return PCCompareResults::Equal;
        loadKey(any_tid, kt, metadata);
      }
      uint8_t key_byte = (k.getKeyLen() > level) ? k[level] : fillKey;

      uint8_t current_byte =
          i >= max_stored_prefix_length ? kt[level] : n->GetPrefix()[i];
      if (current_byte < key_byte) {
        return PCCompareResults::Smaller;
      } else if (current_byte > key_byte) {
        return PCCompareResults::Bigger;
      }
      ++level;
    }
  }
  return PCCompareResults::Equal;
}

typename AdaptiveRadixTree::PCEqualsResults
AdaptiveRadixTree::CheckPrefixEquals(const N *n, uint32_t &level,
                                     const ARTKey &start, const ARTKey &end,
                                     LoadKeyFunction loadKey,
                                     bool &need_restart,
                                     IndexMetadata *metadata) {
  if (n->HasPrefix()) {
    ARTKey kt;
    for (uint32_t i = 0; i < n->GetPrefixLength(); ++i) {
      if (i == max_stored_prefix_length) {
        auto any_tid = N::GetAnyChildTid(n, need_restart);
        if (need_restart) return PCEqualsResults::BothMatch;
        loadKey(any_tid, kt, metadata);
      }
      uint8_t start_level = (start.getKeyLen() > level) ? start[level] : 0;
      uint8_t end_level = (end.getKeyLen() > level) ? end[level] : 255;

      uint8_t current_byte =
          i >= max_stored_prefix_length ? kt[level] : n->GetPrefix()[i];
      if (current_byte > start_level && current_byte < end_level) {
        return PCEqualsResults::Contained;
      } else if (current_byte < start_level || current_byte > end_level) {
        return PCEqualsResults::NoMatch;
      }
      ++level;
    }
  }
  return PCEqualsResults::BothMatch;
}
}
}
