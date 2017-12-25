//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_node.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_N_H
#define PELOTON_N_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/art_key.h"
#include "index/art_epoch_manager.h"

namespace peloton {
namespace index {

using TID = uint64_t;

/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

typedef struct MultiValues {
  TID tid;
  std::atomic<uint64_t> next;
} MultiValues;

static constexpr uint32_t max_stored_prefix_length = 11;

using Prefix = uint8_t[max_stored_prefix_length];

class N {
 protected:
  N(NTypes type, const uint8_t *prefix, uint32_t prefixLength) {
    SetType(type);
    SetPrefix(prefix, prefixLength);
  }

  N(const N &) = delete;

  N(N &&) = delete;

  // 2b type 60b version 1b lock 1b obsolete
  std::atomic<uint64_t> type_version_lock_obsolete_{0b100};
  // version 1, unlocked, not obsolete
  uint32_t prefix_count_ = 0;

  uint8_t count_ = 0;
  Prefix prefix_;

  void SetType(NTypes type);

  static uint64_t ConvertTypeToVersion(NTypes type);

 public:
  NTypes GetType() const;

  uint32_t GetCount() const;

  bool IsLocked(uint64_t version) const;

  void WriteLockOrRestart(bool &needRestart);

  void UpgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart);

  void WriteUnlock();

  uint64_t ReadLockOrRestart(bool &needRestart) const;

  /**
   * returns true if node hasn't been changed in between
   */
  void CheckOrRestart(uint64_t startRead, bool &needRestart) const;
  void ReadUnlockOrRestart(uint64_t startRead, bool &needRestart) const;

  static bool IsObsolete(uint64_t version);

  /**
   * can only be called when node is locked
   */
  void WriteUnlockObsolete() { type_version_lock_obsolete_.fetch_add(0b11); }

  static N *GetChild(const uint8_t k, const N *node);

  static void InsertAndUnlock(N *node, uint64_t v, N *parentNode,
                              uint64_t parentVersion, uint8_t keyParent,
                              uint8_t key, N *val, bool &needRestart,
                              ThreadInfo &threadInfo);

  static bool Change(N *node, uint8_t key, N *val);

  static bool AddMultiValue(N *node, uint8_t key, uint64_t val);

  static void RemoveAndUnlock(N *node, uint64_t v, uint8_t key, N *parentNode,
                              uint64_t parentVersion, uint8_t keyParent,
                              bool &needRestart, ThreadInfo &threadInfo);

  static void RemoveLockedNodeAndUnlock(N *node, uint8_t key, N *parentNode,
                                        uint64_t parentVersion,
                                        uint8_t keyParent, bool &needRestart,
                                        ThreadInfo &threadInfo);

  bool HasPrefix() const;

  const uint8_t *GetPrefix() const;

  void SetPrefix(const uint8_t *prefix, uint32_t length);

  void AddPrefixBefore(N *node, uint8_t key);

  uint32_t GetPrefixLength() const;

  static TID GetLeaf(const N *n);

  static bool IsLeaf(const N *n);

  static N *SetLeaf(TID tid);

  static N *SetLeafWithListPointer(MultiValues *value_list);

  static N *GetAnyChild(const N *n);

  static TID GetAnyChildTid(const N *n, bool &needRestart);

  static void DeleteChildren(N *node);

  static void DeleteNode(N *node);

  static std::tuple<N *, uint8_t> GetSecondChild(N *node, const uint8_t k);

  template <typename curN, typename biggerN>
  static void InsertGrow(curN *n, uint64_t v, N *parentNode,
                         uint64_t parentVersion, uint8_t keyParent, uint8_t key,
                         N *val, bool &needRestart, ThreadInfo &threadInfo);

  template <typename curN, typename smallerN>
  static void RemoveAndShrink(curN *n, uint64_t v, N *parentNode,
                              uint64_t parentVersion, uint8_t keyParent,
                              uint8_t key, bool &needRestart,
                              ThreadInfo &threadInfo);

  template <typename curN, typename smallerN>
  static void RemoveLockedNodeAndShrink(curN *n, N *parentNode,
                                        uint64_t parentVersion,
                                        uint8_t keyParent, uint8_t key,
                                        bool &needRestart,
                                        ThreadInfo &threadInfo);

  static uint64_t GetChildren(const N *node, uint8_t start, uint8_t end,
                              std::tuple<uint8_t, N *> children[],
                              uint32_t &childrenCount);
};
}
}

#endif  // PELOTON_N_H
