//
// Created by Min Huang on 9/20/17.
//

#include <assert.h>
#include <algorithm>
#include <emmintrin.h>
#include <functional>
#include "index/art.h"
#include "index/Key.h"
#include "index/Epoche.h"
#include "index/N.h"
#include "index/N4.h"
#include "index/N16.h"
#include "index/N48.h"
#include "index/N256.h"
#include "index/index.h"

namespace peloton {
namespace index {


Tree::Tree(LoadKeyFunction loadKey) : root(new N256( nullptr, 0)), loadKey(loadKey), epoche(256) {

}

Tree::~Tree() {
  N::deleteChildren(root);
  N::deleteNode(root);
}

ThreadInfo &Tree::getThreadInfo() {
  static thread_local int gc_id = (this->epoche).thread_info_counter++;
  return (this->epoche).getThreadInfoByID(gc_id);
//  return ThreadInfo(this->epoche);
}

void Tree::setIndexMetadata(IndexMetadata *metadata) {
  this->metadata = metadata;
}

void yield(int count) {
  if (count>3)
    sched_yield();
  else
    _mm_pause();
}

TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
  EpocheGuardReadonly epocheGuard(threadEpocheInfo);
  int restartCount = 0;
  restart:
  if (restartCount++)
    yield(restartCount);
  bool needRestart = false;

  N *node;
  N *parentNode = nullptr;
  uint64_t v;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  node = root;
  v = node->readLockOrRestart(needRestart);
  if (needRestart) goto restart;
  while (true) {
    switch (checkPrefix(node, k, level)) { // increases level
      case CheckPrefixResult::NoMatch:
        node->readUnlockOrRestart(v, needRestart);
        if (needRestart) goto restart;
        return 0;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          return 0;
        }
        parentNode = node;
        node = N::getChild(k[level], parentNode);
        parentNode->checkOrRestart(v,needRestart);
        if (needRestart) goto restart;

        if (node == nullptr) {
          return 0;
        }
        if (N::isLeaf(node)) {
          parentNode->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;

          TID tid = N::getLeaf(node);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            return checkKey(tid, k);
          }
          return tid;
        }
        level++;
    }
    uint64_t nv = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    parentNode->readUnlockOrRestart(v, needRestart);
    if (needRestart) goto restart;
    v = nv;
  }
}

bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, std::vector<ItemPointer *> &result,
                       std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
  for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
    if (start[i] > end[i]) {
      resultsFound = 0;
      return false;
    } else if (start[i] < end[i]) {
      break;
    }
  }
  EpocheGuard epocheGuard(threadEpocheInfo);
  TID toContinue = 0;
  std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
    if (N::isLeaf(node)) {
//      if (resultsFound == resultSize) {
//        toContinue = N::getLeaf(node);
//        return;
//      }
//      result[resultsFound] = N::getLeaf(node);

      TID tid = N::getLeaf(node);
      MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
      while (value_list != nullptr) {
        ItemPointer *new_value = (ItemPointer *)(value_list->tid);
        result.push_back(new_value);
        resultsFound++;
        value_list = value_list->next;
      }

//      result.push_back((ItemPointer *)(N::getLeaf(node)));
//      resultsFound++;
    } else {
      std::tuple<uint8_t, N *> children[256];
      uint32_t childrenCount = 0;
      N::getChildren(node, 0u, 255u, children, childrenCount);
      for (uint32_t i = 0; i < childrenCount; ++i) {
        const N *n = std::get<1>(children[i]);
        copy(n);
        if (toContinue != 0) {
          break;
        }
      }
    }
  };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart = [&copy, &start, &findStart, &toContinue, this](
    N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
    if (N::isLeaf(node)) {
      copy(node);
      return;
    }
    uint64_t v;
    PCCompareResults prefixResult;

    {
      readAgain:
      bool needRestart = false;
      v = node->readLockOrRestart(needRestart);
      if (needRestart) goto readAgain;

      prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart, metadata);
      if (needRestart) goto readAgain;

      parentNode->readUnlockOrRestart(vp, needRestart);
      if (needRestart) {
        readParentAgain:
        needRestart = false;
        vp = parentNode->readLockOrRestart(needRestart);
        if (needRestart) goto readParentAgain;

        node = N::getChild(nodeK, parentNode);

        parentNode->readUnlockOrRestart(vp, needRestart);
        if (needRestart) goto readParentAgain;

        if (node == nullptr) {
          return;
        }
        if (N::isLeaf(node)) {
          copy(node);
          return;
        }
        goto readAgain;
      }
      node->readUnlockOrRestart(v, needRestart);
      if (needRestart) goto readAgain;
    }

    switch (prefixResult) {
      case PCCompareResults::Bigger:
        copy(node);
        break;
      case PCCompareResults::Equal: {
        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        std::tuple<uint8_t, N *> children[256];
        uint32_t childrenCount = 0;
        v = N::getChildren(node, startLevel, 255, children, childrenCount);
        for (uint32_t i = 0; i < childrenCount; ++i) {
          const uint8_t k = std::get<0>(children[i]);
          N *n = std::get<1>(children[i]);
          if (k == startLevel) {
            findStart(n, k, level + 1, node, v);
          } else if (k > startLevel) {
            copy(n);
          }
          if (toContinue != 0) {
            break;
          }
        }
        break;
      }
      case PCCompareResults::Smaller:
        break;
    }
  };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd = [&copy, &end, &toContinue, &findEnd, this](
    N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
    if (N::isLeaf(node)) {
      // end boundary inclusive
      copy(node);
      return;
    }
    uint64_t v;
    PCCompareResults prefixResult;
    {
      readAgain:
      bool needRestart = false;
      v = node->readLockOrRestart(needRestart);
      if (needRestart) goto readAgain;

      prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart, metadata);
      if (needRestart) goto readAgain;

      parentNode->readUnlockOrRestart(vp, needRestart);
      if (needRestart) {
        readParentAgain:
        vp = parentNode->readLockOrRestart(needRestart);
        if (needRestart) goto readParentAgain;

        node = N::getChild(nodeK, parentNode);

        parentNode->readUnlockOrRestart(vp, needRestart);
        if (needRestart) goto readParentAgain;

        if (node == nullptr) {
          return;
        }
        if (N::isLeaf(node)) {
          return;
        }
        goto readAgain;
      }
      node->readUnlockOrRestart(v, needRestart);
      if (needRestart) goto readAgain;
    }
    switch (prefixResult) {
      case PCCompareResults::Smaller:
        copy(node);
        break;
      case PCCompareResults::Equal: {
        uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
        std::tuple<uint8_t, N *> children[256];
        uint32_t childrenCount = 0;
        v = N::getChildren(node, 0, endLevel, children, childrenCount);
        for (uint32_t i = 0; i < childrenCount; ++i) {
          const uint8_t k = std::get<0>(children[i]);
          N *n = std::get<1>(children[i]);
          if (k == endLevel) {
            findEnd(n, k, level + 1, node, v);
          } else if (k < endLevel) {
            copy(n);
          }
          if (toContinue != 0) {
            break;
          }
        }
        break;
      }
      case PCCompareResults::Bigger:
        break;
    }
  };


  int restartCount = 0;
  restart:
  if (restartCount++)
    yield(restartCount);
  bool needRestart = false;

  resultsFound = 0;

  uint32_t level = 0;
  N *node = nullptr;
  N *nextNode = root;
  N *parentNode;
  uint64_t v = 0;
  uint64_t vp;

  while (true) {
    parentNode = node;
    vp = v;
    node = nextNode;
    PCEqualsResults prefixResult;
    v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
    prefixResult = checkPrefixEquals(node, level, start, end, loadKey, needRestart, metadata);
    if (needRestart) goto restart;
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(vp, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(v, needRestart);
    if (needRestart) goto restart;

    switch (prefixResult) {
      case PCEqualsResults::NoMatch: {
        return false;
      }
      case PCEqualsResults::Contained: {
        copy(node);
        break;
      }
      case PCEqualsResults::BothMatch: {
        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
        if (startLevel != endLevel) {
          std::tuple<uint8_t, N *> children[256];
          uint32_t childrenCount = 0;
          v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
          for (uint32_t i = 0; i < childrenCount; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            N *n = std::get<1>(children[i]);
            if (k == startLevel) {
              findStart(n, k, level + 1, node, v);
            } else if (k > startLevel && k < endLevel) {
              copy(n);
            } else if (k == endLevel) {
              findEnd(n, k, level + 1, node, v);
            }
            if (toContinue) {
              break;
            }
          }
        } else {
          nextNode = N::getChild(startLevel, node);
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          level++;
          continue;
        }
        break;
      }
    }
    break;
  }
  if (toContinue != 0) {
    loadKey(toContinue, continueKey, metadata);
    return true;
  } else {
    return false;
  }
}

void Tree::scanAllLeafNodes(const N* node, std::vector<ItemPointer *> &result, std::size_t &resultCount) const {
  if (N::isLeaf(node)) {
    TID tid = N::getLeaf(node);
    MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);
    while (value_list != nullptr) {
      ItemPointer *new_value = (ItemPointer *)(value_list->tid);
      result.push_back(new_value);
      resultCount++;
      value_list = value_list->next;
    }
  } else {
    std::tuple<uint8_t, N *> children[256];
    uint32_t childrenCount = 0;
    N::getChildren(node, 0u, 255u, children, childrenCount);
    for (uint32_t i = 0; i < childrenCount; ++i) {
      const N *n = std::get<1>(children[i]);
      scanAllLeafNodes(n, result, resultCount);
    }
  }
}

void Tree::fullScan(std::vector<ItemPointer *> &result, std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const {
  EpocheGuard epocheGuard(threadEpocheInfo);
  scanAllLeafNodes(root, result, resultCount);
}

TID Tree::checkKey(const TID tid, const Key &k) const {
  Key kt;
  printf("in checkKey()\n");
  this->loadKey(tid, kt, metadata);
  if (k == kt) {
    printf("recovered key is the same as the inserted key\n");
    return tid;
  }
  printf("recovered key is not the same as the inserted key!\n");
  return 0;
}

void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo, bool &insertSuccess) {
  EpocheGuard epocheGuard(epocheInfo);
  int restartCount = 0;
  insertSuccess = false;
  restart:
  if (restartCount++)
    yield(restartCount);
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    uint32_t nextLevel = level;

    uint8_t nonMatchingKey;
    Prefix remainingPrefix;
    auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                      this->loadKey, needRestart, metadata); // increases level
    if (needRestart) goto restart;
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
        if (needRestart) goto restart;

        node->upgradeToWriteLockOrRestart(v, needRestart);
        if (needRestart) {
          parentNode->writeUnlock();
          goto restart;
        }
        // 1) Create new node which will be parent of node, Set common prefix, level to this node
        auto newNode = new N4(node->getPrefix(), nextLevel - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[nextLevel], N::setLeaf(tid));
        newNode->insert(nonMatchingKey, node);

        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
        N::change(parentNode, parentKey, newNode);
        parentNode->writeUnlock();

        // 4) update prefix of node, unlock
        node->setPrefix(remainingPrefix,
                        node->getPrefixLength() - ((nextLevel - level) + 1));

        node->writeUnlock();
        insertSuccess = true;
        return;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = nextLevel;
    nodeKey = k[level];
    nextNode = N::getChild(nodeKey, node);
    node->checkOrRestart(v,needRestart);
    if (needRestart) goto restart;

    if (nextNode == nullptr) {
      N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, epocheInfo);
      if (needRestart) goto restart;
      insertSuccess = true;
      return;
    }

    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) goto restart;
    }

    if (N::isLeaf(nextNode)) {
      node->upgradeToWriteLockOrRestart(v, needRestart);
      if (needRestart) goto restart;

      Key key;
      loadKey(N::getLeaf(nextNode), key, metadata);

      if (key == k) {
        // upsert
//        N::change(node, k[level], N::setLeaf(tid));
//        printf("add another value for a non-unique key %llu\n", tid);
//        N::addMultiValue(node, k[level], N::setLeaf(tid));
        N::addMultiValue(node, k[level], tid);
        node->writeUnlock();
        insertSuccess = true;
        return;
      }

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = new N4(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], N::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNode);
      N::change(node, k[level - 1], n4);
      node->writeUnlock();
      insertSuccess = true;
      return;
    }
    level++;
    parentVersion = v;
  }
}

bool Tree::conditionalInsert(const Key &k, TID tid, ThreadInfo &epocheInfo, std::function<bool(const void *)> predicate) {
  EpocheGuard epocheGuard(epocheInfo);
  int restartCount = 0;
  restart:
  if (restartCount++)
    yield(restartCount);
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    uint32_t nextLevel = level;

    uint8_t nonMatchingKey;
    Prefix remainingPrefix;
    auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                      this->loadKey, needRestart, metadata); // increases level
    if (needRestart) goto restart;
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
        if (needRestart) goto restart;

        node->upgradeToWriteLockOrRestart(v, needRestart);
        if (needRestart) {
          parentNode->writeUnlock();
          goto restart;
        }
        // 1) Create new node which will be parent of node, Set common prefix, level to this node
        auto newNode = new N4(node->getPrefix(), nextLevel - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[nextLevel], N::setLeaf(tid));
        newNode->insert(nonMatchingKey, node);

        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
        N::change(parentNode, parentKey, newNode);
        parentNode->writeUnlock();

        // 4) update prefix of node, unlock
        node->setPrefix(remainingPrefix,
                        node->getPrefixLength() - ((nextLevel - level) + 1));

        node->writeUnlock();
        return true;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = nextLevel;
    nodeKey = k[level];
    nextNode = N::getChild(nodeKey, node);
    node->checkOrRestart(v,needRestart);
    if (needRestart) goto restart;

    if (nextNode == nullptr) {
      N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, epocheInfo);
      if (needRestart) goto restart;
      return true;
    }

    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) goto restart;
    }

    if (N::isLeaf(nextNode)) {
      node->upgradeToWriteLockOrRestart(v, needRestart);
      if (needRestart) goto restart;

      Key key;
      loadKey(N::getLeaf(nextNode), key, metadata);

      if (key == k) {
        MultiValues *value_list = reinterpret_cast<MultiValues *>(N::getLeaf(nextNode));
        while (value_list != nullptr) {
          ItemPointer *value_pointer = (ItemPointer *) (value_list->tid);
          if (predicate(value_pointer)) {
            node->writeUnlock();
            return false;
          } else if ((uint64_t) value_pointer == tid) {
            node->writeUnlock();
            return false;
          }
          value_list = value_list->next;
        }

        // upsert
//        N::change(node, k[level], N::setLeaf(tid));
//        printf("add another value for a non-unique key %llu\n", tid);
//        N::addMultiValue(node, k[level], N::setLeaf(tid));
        N::addMultiValue(node, k[level], tid);
        node->writeUnlock();
        return true;
      }

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = new N4(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], N::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNode);
      N::change(node, k[level - 1], n4);
      node->writeUnlock();
      return true;
    }
    level++;
    parentVersion = v;
  }
}

void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
//  printf("begin remove\n");
  EpocheGuard epocheGuard(threadInfo);
  int restartCount = 0;
  restart:
  if (restartCount++)
    yield(restartCount);
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    switch (checkPrefix(node, k, level)) { // increases level
      case CheckPrefixResult::NoMatch:
        node->readUnlockOrRestart(v, needRestart);
        if (needRestart) goto restart;
        return;
      case CheckPrefixResult::OptimisticMatch:
        // fallthrough
      case CheckPrefixResult::Match: {
        nodeKey = k[level];
        nextNode = N::getChild(nodeKey, node);

        node->checkOrRestart(v, needRestart);
        if (needRestart) goto restart;

        if (nextNode == nullptr) {
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          return;
        }
        if (N::isLeaf(nextNode)) {
//          if (N::getLeaf(nextNode) != tid) {
//            return;
//          }
          node->upgradeToWriteLockOrRestart(v, needRestart);
          if (needRestart) goto restart;

          MultiValues *parent_value = nullptr;
          MultiValues *value_list = reinterpret_cast<MultiValues *>(N::getLeaf(nextNode));
          uint32_t value_count = 0;
          bool value_found = false;
          while (value_list != nullptr) {
            value_count++;
            if (value_list->tid == tid) {
              value_found = true;
              if (value_list->next != nullptr) {
                value_count++;
              }
              break;
            }
            parent_value = value_list;
            value_list = value_list->next;
          }

          if (!value_found) {
            node->writeUnlock();
            return;
          }

          if (value_count > 1) {

            if (parent_value != nullptr) {
//              printf("deleted value is not the head value\n");
              if (value_list == nullptr) {
                printf("wtf!! r u kidding me?\n");
              }
              parent_value->next = value_list->next;
              value_list->next = nullptr;
              delete value_list;
            } else {
//              printf("deleted value is the head value\n");
              N::change(node, k[level], N::setLeafWithListPointer(value_list->next));
              value_list->next = nullptr;
              delete value_list;
            }

            // remember to unlock the node!!
            node->writeUnlock();
//            printf("unlock the node and carry on\n");
          } else {
            // last value in the value_list is deleted
            assert(parentNode == nullptr || node->getCount() != 1);
            if (node->getCount() == 2 && parentNode != nullptr) {
              parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
              if (needRestart) {
                node->writeUnlock();
                goto restart;
              }

//              node->upgradeToWriteLockOrRestart(v, needRestart);
//              if (needRestart) {
//                parentNode->writeUnlock();
//                goto restart;
//              }
              // 1. check remaining entries
              N *secondNodeN;
              uint8_t secondNodeK;
              std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
              if (N::isLeaf(secondNodeN)) {
                //N::remove(node, k[level]); not necessary
                N::change(parentNode, parentKey, secondNodeN);

                parentNode->writeUnlock();
                node->writeUnlockObsolete();
                this->epoche.markNodeForDeletion(node, threadInfo);
              } else {
                secondNodeN->writeLockOrRestart(needRestart);
                if (needRestart) {
                  node->writeUnlock();
                  parentNode->writeUnlock();
                  goto restart;
                }

                //N::remove(node, k[level]); not necessary
                N::change(parentNode, parentKey, secondNodeN);
                parentNode->writeUnlock();

                secondNodeN->addPrefixBefore(node, secondNodeK);
                secondNodeN->writeUnlock();

                node->writeUnlockObsolete();
                this->epoche.markNodeForDeletion(node, threadInfo);
              }
            } else {
//              N::removeAndUnlock(node, v, k[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
              N::removeLockedNodeAndUnlock(node, k[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
              if (needRestart) goto restart;
            }
          }



          return;
        }
        level++;
        parentVersion = v;
      }
    }
  }
}

inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
  if (n->hasPrefix()) {
    if (k.getKeyLen() <= level + n->getPrefixLength()) {
      return CheckPrefixResult::NoMatch;
    }
    for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
      if (n->getPrefix()[i] != k[level]) {
        return CheckPrefixResult::NoMatch;
      }
      ++level;
    }
    if (n->getPrefixLength() > maxStoredPrefixLength) {
      level = level + (n->getPrefixLength() - maxStoredPrefixLength);
      return CheckPrefixResult::OptimisticMatch;
    }
  }
  return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                         uint8_t &nonMatchingKey,
                                                                         Prefix &nonMatchingPrefix,
                                                                         LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata) {
  if (n->hasPrefix()) {
    uint32_t prevLevel = level;
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return CheckPrefixPessimisticResult::Match;
        loadKey(anyTID, kt, metadata);
      }
      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey != k[level]) {
        nonMatchingKey = curKey;
        if (n->getPrefixLength() > maxStoredPrefixLength) {
          if (i < maxStoredPrefixLength) {
            auto anyTID = N::getAnyChildTid(n, needRestart);
            if (needRestart) return CheckPrefixPessimisticResult::Match;
            loadKey(anyTID, kt, metadata);
          }
          memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                                 maxStoredPrefixLength));
        } else {
          memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, n->getPrefixLength() - i - 1);
        }
        return CheckPrefixPessimisticResult::NoMatch;
      }
      ++level;
    }
  }
  return CheckPrefixPessimisticResult::Match;
}

typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                         LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return PCCompareResults::Equal;
        loadKey(anyTID, kt, metadata);
      }
      uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey < kLevel) {
        return PCCompareResults::Smaller;
      } else if (curKey > kLevel) {
        return PCCompareResults::Bigger;
      }
      ++level;
    }
  }
  return PCCompareResults::Equal;
}

typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                       LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return PCEqualsResults::BothMatch;
        loadKey(anyTID, kt, metadata);
      }
      uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
      uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey > startLevel && curKey < endLevel) {
        return PCEqualsResults::Contained;
      } else if (curKey < startLevel || curKey > endLevel) {
        return PCEqualsResults::NoMatch;
      }
      ++level;
    }
  }
  return PCEqualsResults::BothMatch;
}



}
}
