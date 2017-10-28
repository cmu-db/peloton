//
// Created by Min Huang on 9/20/17.
//

#ifndef PELOTON_N_H
#define PELOTON_N_H

#pragma once

#include <stdint.h>
#include <atomic>
#include <string.h>
#include "index/Key.h"
#include "index/Epoche.h"

namespace peloton {
namespace index {

using TID = uint64_t;

/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */

enum class NTypes : uint8_t {
  N4 = 0,
  N16 = 1,
  N48 = 2,
  N256 = 3
};

typedef struct MultiValues {
  TID tid;
  MultiValues *next;
} MultiValues;

static constexpr uint32_t maxStoredPrefixLength = 11;

using Prefix = uint8_t[maxStoredPrefixLength];

class N {
protected:
  N(NTypes type, const uint8_t *prefix, uint32_t prefixLength) {
    setType(type);
    setPrefix(prefix, prefixLength);
  }

  N(const N &) = delete;

  N(N &&) = delete;

  //2b type 60b version 1b lock 1b obsolete
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};
  // version 1, unlocked, not obsolete
  uint32_t prefixCount = 0;

  uint8_t count = 0;
  Prefix prefix;


  void setType(NTypes type);

  static uint64_t convertTypeToVersion(NTypes type);

public:

  NTypes getType() const;

  uint32_t getCount() const;

  bool isLocked(uint64_t version) const;

  void writeLockOrRestart(bool &needRestart);

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart);

  void writeUnlock();

  uint64_t readLockOrRestart(bool &needRestart) const;

  /**
   * returns true if node hasn't been changed in between
   */
  void checkOrRestart(uint64_t startRead, bool &needRestart) const;
  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const;

  static bool isObsolete(uint64_t version);

  /**
   * can only be called when node is locked
   */
  void writeUnlockObsolete() {
    typeVersionLockObsolete.fetch_add(0b11);
  }

  static N *getChild(const uint8_t k, const N *node);

  static void insertAndUnlock(N *node, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart,
                              ThreadInfo &threadInfo);

  static bool change(N *node, uint8_t key, N *val);

  static bool addMultiValue(N *node, uint8_t key, uint64_t val);

  static void removeAndUnlock(N *node, uint64_t v, uint8_t key, N *parentNode, uint64_t parentVersion, uint8_t keyParent, bool &needRestart, ThreadInfo &threadInfo);

  bool hasPrefix() const;

  const uint8_t *getPrefix() const;

  void setPrefix(const uint8_t *prefix, uint32_t length);

  void addPrefixBefore(N *node, uint8_t key);

  uint32_t getPrefixLength() const;

  static TID getLeaf(const N *n);

  static bool isLeaf(const N *n);

  static N *setLeaf(TID tid);

  static N *setLeafWithListPointer(MultiValues *value_list);

  static N *getAnyChild(const N *n);

  static TID getAnyChildTid(const N *n, bool &needRestart);

  static void deleteChildren(N *node);

  static void deleteNode(N *node);

  static std::tuple<N *, uint8_t> getSecondChild(N *node, const uint8_t k);

  template<typename curN, typename biggerN>
  static void insertGrow(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, N *val, bool &needRestart, ThreadInfo &threadInfo);

  template<typename curN, typename smallerN>
  static void removeAndShrink(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent, uint8_t key, bool &needRestart, ThreadInfo &threadInfo);

  static uint64_t getChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                              uint32_t &childrenCount);
};


}
}


#endif //PELOTON_N_H
