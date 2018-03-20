#include <algorithm>
#include <cassert>
#include <xmmintrin.h>  // For _mm_pause()

#include "Tree.h"
#include "Node_impl.h"
#include "Epoch_impl.h"

namespace art {

Tree::Tree(LoadKeyFunction loadKey, void *ctx)
    : root(new Node256(nullptr, 0)), keyLoader(loadKey, ctx), epoch(256) {}

Tree::~Tree() {
  Node::deleteChildren(root);
  Node::deleteNode(root);
}

ThreadInfo Tree::getThreadInfo() { return ThreadInfo(epoch); }

void yield(int count) {
  if (count > 3) {
    sched_yield();
  } else {
    _mm_pause();
  }
}

TID Tree::checkKey(const TID tid, const Key &k) const {
  Key kt;
  keyLoader.load(tid, kt);
  return (k == kt ? tid : 0);
}

void Tree::setLoadKeyFunc(Tree::LoadKeyFunction loadKey, void *ctx) {
  keyLoader.reset(loadKey, ctx);
}

bool Tree::lookup(const Key &k, std::vector<TID> &results,
                  ThreadInfo &threadEpochInfo) const {
  EpochGuardReadonly epochGuard(threadEpochInfo);
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  Node *node;
  Node *parentNode = nullptr;
  uint64_t v;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  node = root;
  v = node->readLockOrRestart(needRestart);
  if (needRestart) goto restart;
  while (true) {
    switch (checkPrefix(node, k, level)) {  // Increases level
      case CheckPrefixResult::NoMatch:
        // Prefix mismatch
        node->readUnlockOrRestart(v, needRestart);
        if (needRestart) goto restart;
        return false;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
      // Fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          return false;
        }
        parentNode = node;
        node = Node::getChild(k[level], parentNode);
        parentNode->checkOrRestart(v, needRestart);
        if (needRestart) goto restart;

        if (node == nullptr) {
          // Not found
          return false;
        }
        if (Node::isLeaf(node)) {
          parentNode->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;

          LeafNode::readLeaf(node, results, needRestart);
          if (needRestart) goto restart;

          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            if (checkKey(results[0], k) == 0) {
              // Optimistic prefix match failed
              results.clear();
              return false;
            }
          }
          // All good
          return true;
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

bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey,
                       std::vector<TID> &results, uint32_t softMaxResults,
                       ThreadInfo &threadEpochInfo) const {
  // No results if start key is greater than end key
  for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
    if (start[i] > end[i]) {
      return false;
    } else if (start[i] < end[i]) {
      break;
    }
  }

  EpochGuard epochGuard(threadEpochInfo);
  TID toContinue = 0;

  // This function copies all leaves in the tree rooted at the provided node
  // into the result vector, stopping if the result size exceeds the limited
  // provided by the caller.
  std::function<void(const Node *, bool &)> copy =
      [&results, &softMaxResults, &toContinue, &copy](const Node *node,
                                                      bool &needRestart) {
        if (Node::isLeaf(node)) {
          if (results.size() >= softMaxResults) {
            toContinue = Node::getLeaf(node);
            return;
          }
          LeafNode::readLeaf(node, results, needRestart);
        } else {
          std::tuple<uint8_t, Node *> children[256];
          uint32_t childrenCount = 0;
          Node::getChildren(node, 0u, 255u, children, childrenCount,
                            needRestart);
          if (needRestart) return;
          for (uint32_t i = 0; i < childrenCount; ++i) {
            const Node *n = std::get<1>(children[i]);
            copy(n, needRestart);
            if (needRestart) return;
            if (toContinue != 0) {
              break;
            }
          }
        }
      };

  std::function<void(Node *, uint8_t, uint32_t, const Node *, uint64_t, bool &)>
      findStart = [&copy, &start, &findStart, &toContinue, this](
          Node *node, uint8_t nodeK, uint32_t level, const Node *parentNode,
          uint64_t vp, bool &needRestart) {
        if (Node::isLeaf(node)) {
          copy(node, needRestart);
          return;
        }
        uint64_t v;
        PCCompareResults prefixResult;

        {
        parentRereadSuccess:
          v = node->readLockOrRestart(needRestart);
          if (needRestart) return;

          prefixResult =
              checkPrefixCompare(node, start, 0, level, keyLoader, needRestart);
          if (needRestart) return;

          parentNode->readUnlockOrRestart(vp, needRestart);
          if (needRestart) {
            // give only one chance to read the parent node again
            needRestart = false;
            vp = parentNode->readLockOrRestart(needRestart);
            if (needRestart) {
              // unlock the read lock on current node
              node->readUnlockOrRestart(v, needRestart);
              // unlock may set needRestart as false, but we ignore it
              needRestart = true;
              return;
            };

            node = Node::getChild(nodeK, parentNode);

            parentNode->readUnlockOrRestart(vp, needRestart);
            if (needRestart) {
              // node is new, so don't readUnlock current node
              return;
            };

            if (node == nullptr) {
              return;
            }
            if (Node::isLeaf(node)) {
              copy(node, needRestart);
              return;
            }
            goto parentRereadSuccess;
          }
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) return;
        }

        switch (prefixResult) {
          case PCCompareResults::Bigger:
            copy(node, needRestart);
            break;
          case PCCompareResults::Equal: {
            uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
            std::tuple<uint8_t, Node *> children[256];
            uint32_t childrenCount = 0;
            v = Node::getChildren(node, startLevel, 255, children,
                                  childrenCount, needRestart);
            if (needRestart) return;
            for (uint32_t i = 0; i < childrenCount; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              Node *n = std::get<1>(children[i]);
              if (k == startLevel) {
                findStart(n, k, level + 1, node, v, needRestart);
                if (needRestart) return;
              } else if (k > startLevel) {
                copy(n, needRestart);
                if (needRestart) return;
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

  std::function<void(Node *, uint8_t, uint32_t, const Node *, uint64_t, bool &)>
      findEnd = [&copy, &end, &toContinue, &findEnd, this](
          Node *node, uint8_t nodeK, uint32_t level, const Node *parentNode,
          uint64_t vp, bool &needRestart) {
        if (Node::isLeaf(node)) {
          copy(node, needRestart);
          return;
        }
        uint64_t v;
        PCCompareResults prefixResult;
        {
        parentRereadSuccess:
          v = node->readLockOrRestart(needRestart);
          if (needRestart) return;

          prefixResult =
              checkPrefixCompare(node, end, 255, level, keyLoader, needRestart);
          if (needRestart) return;

          parentNode->readUnlockOrRestart(vp, needRestart);
          if (needRestart) {
            // one change
            vp = parentNode->readLockOrRestart(needRestart);
            if (needRestart) {
              node->readUnlockOrRestart(v, needRestart);
              needRestart = true;
              return;
            };

            node = Node::getChild(nodeK, parentNode);

            parentNode->readUnlockOrRestart(vp, needRestart);
            if (needRestart) return;

            if (node == nullptr) {
              return;
            }
            if (Node::isLeaf(node)) {
              return;
            }
            goto parentRereadSuccess;
          }
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) return;
        }
        switch (prefixResult) {
          case PCCompareResults::Smaller:
            copy(node, needRestart);
            break;
          case PCCompareResults::Equal: {
            uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
            std::tuple<uint8_t, Node *> children[256];
            uint32_t childrenCount = 0;
            v = Node::getChildren(node, 0, endLevel, children, childrenCount,
                                  needRestart);
            if (needRestart) return;
            for (uint32_t i = 0; i < childrenCount; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              Node *n = std::get<1>(children[i]);
              if (k == endLevel) {
                findEnd(n, k, level + 1, node, v, needRestart);
                if (needRestart) return;
              } else if (k < endLevel) {
                copy(n, needRestart);
                if (needRestart) return;
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
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  // Every restart means to clear the results we've collected so far
  // TODO(pmenon): We just need to restart search at last failed key
  results.clear();

  uint32_t level = 0;
  Node *node = nullptr;
  Node *nextNode = root;
  Node *parentNode;
  uint64_t v = 0;
  uint64_t vp;

  while (true) {
    parentNode = node;
    vp = v;
    node = nextNode;
    v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    // Check prefix
    PCEqualsResults prefixResult =
        checkPrefixEquals(node, level, start, end, keyLoader, needRestart);
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
        copy(node, needRestart);
        if (needRestart) goto restart;
        break;
      }
      case PCEqualsResults::BothMatch: {
        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
        if (startLevel != endLevel) {
          std::tuple<uint8_t, Node *> children[256];
          uint32_t childrenCount = 0;
          v = Node::getChildren(node, startLevel, endLevel, children,
                                childrenCount, needRestart);
          for (uint32_t i = 0; i < childrenCount; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            Node *n = std::get<1>(children[i]);
            if (k == startLevel) {
              findStart(n, k, level + 1, node, v, needRestart);
              if (needRestart) goto restart;
            } else if (k > startLevel && k < endLevel) {
              copy(n, needRestart);
              if (needRestart) goto restart;
            } else if (k == endLevel) {
              findEnd(n, k, level + 1, node, v, needRestart);
              if (needRestart) goto restart;
            }
            if (toContinue) {
              break;
            }
          }
        } else {
          nextNode = Node::getChild(startLevel, node);
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;

          if (nextNode == nullptr) {
            // Not found
            return false;
          }

          if (Node::isLeaf(nextNode)) {
            // Copy single leaf node
            copy(nextNode, needRestart);
            if (needRestart) goto restart;
            return false;
          }

          level++;
          continue;
        }
        break;
      }
    }
    break;
  }
  if (toContinue != 0) {
    keyLoader.load(toContinue, continueKey);
    return true;
  } else {
    return false;
  }
}

bool Tree::insert(const Key &k, TID tid, ThreadInfo &epochInfo) {
  return conditionalInsert(k, tid, nullptr, epochInfo);
}

bool Tree::conditionalInsert(const Key &k, TID tid,
                             std::function<bool(const void *)> predicate,
                             ThreadInfo &epochInfo) {
  EpochGuard epochGuard(epochInfo);
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  Node *node = nullptr;
  Node *nextNode = root;
  Node *parentNode = nullptr;
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
    Prefix remainingPrefix = {0};
    auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
                                      remainingPrefix, keyLoader,
                                      needRestart);  // increases level
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
        // 1) Create a new node which will be parent of the current node. Set
        //    common prefix and level to this node.
        auto newNode = new Node4(node->getPrefix(), nextLevel - level);

        // 2)  Add node and (*k, tid) as children
        newNode->insert(k[nextLevel], Node::setLeaf(tid));
        newNode->insert(nonMatchingKey, node);

        // 3) UpgradeToWriteLockOrRestart, update parentNode to point to the new
        //    node, unlock
        Node::change(parentNode, parentKey, Node::setNonLeaf(newNode));
        parentNode->writeUnlock();

        // 4) Update prefix of node, unlock
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
    nextNode = Node::getChild(nodeKey, node);
    node->checkOrRestart(v, needRestart);
    if (needRestart) goto restart;

    if (nextNode == nullptr) {
      Node::insertAndUnlock(node, v, parentNode, parentVersion, parentKey,
                            nodeKey, Node::setLeaf(tid), needRestart,
                            epochInfo);
      if (needRestart) goto restart;
      return true;
    }

    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) goto restart;
    }

    if (Node::isLeaf(nextNode)) {
      Key key;
      keyLoader.load(Node::getLeaf(nextNode), key);

      if (key == k) {
        bool inserted = LeafNode::insertGrow(nextNode, tid, predicate, k[level],
                                             node, v, needRestart, epochInfo);
        if (needRestart) goto restart;
        return inserted;
      }

      node->upgradeToWriteLockOrRestart(v, needRestart);
      if (needRestart) goto restart;

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = new Node4(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], Node::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNode);
      Node::change(node, k[level - 1], Node::setNonLeaf(n4));
      node->writeUnlock();
      return true;
    }
    level++;
    parentVersion = v;
  }
}

bool Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
  EpochGuard epochGuard(threadInfo);
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  Node *node = nullptr;
  Node *nextNode = root;
  Node *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        node->readUnlockOrRestart(v, needRestart);
        if (needRestart) goto restart;
        return false;
      case CheckPrefixResult::OptimisticMatch:
      // fallthrough
      case CheckPrefixResult::Match: {
        nodeKey = k[level];
        nextNode = Node::getChild(nodeKey, node);

        node->checkOrRestart(v, needRestart);
        if (needRestart) goto restart;

        if (nextNode == nullptr) {
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          return false;
        }
        if (Node::isLeaf(nextNode)) {
          if (LeafNode::isInlined(nextNode) && Node::getLeaf(nextNode) != tid) {
            return false;
          } else if (LeafNode::isExternal(nextNode)) {
            return LeafNode::removeShrink(nextNode, tid, k[level], node, v,
                                          needRestart, threadInfo);
          }

          assert(parentNode == nullptr || node->getCount() != 1);
          if (node->getCount() == 2 && parentNode != nullptr) {
            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
            if (needRestart) goto restart;

            node->upgradeToWriteLockOrRestart(v, needRestart);
            if (needRestart) {
              parentNode->writeUnlock();
              goto restart;
            }
            // 1. check remaining entries
            Node *secondNodeN;
            uint8_t secondNodeK;
            std::tie(secondNodeN, secondNodeK) =
                Node::getSecondChild(node, nodeKey);
            if (Node::isLeaf(secondNodeN)) {
              // N::remove(node, k[level]); not necessary
              Node::change(parentNode, parentKey, secondNodeN);

              parentNode->writeUnlock();
              node->writeUnlockObsolete();
              epoch.markNodeForDeletion(node, threadInfo);
            } else {
              secondNodeN->writeLockOrRestart(needRestart);
              if (needRestart) {
                node->writeUnlock();
                parentNode->writeUnlock();
                goto restart;
              }

              // N::remove(node, k[level]); not necessary
              Node::change(parentNode, parentKey, secondNodeN);
              parentNode->writeUnlock();

              secondNodeN->addPrefixBefore(node, secondNodeK);
              secondNodeN->writeUnlock();

              node->writeUnlockObsolete();
              epoch.markNodeForDeletion(node, threadInfo);
            }
          } else {
            Node::removeAndUnlock(node, v, k[level], parentNode, parentVersion,
                                  parentKey, needRestart, threadInfo);
            if (needRestart) goto restart;
          }
          return true;
        }
        level++;
        parentVersion = v;
      }
    }
  }
}

Tree::CheckPrefixResult Tree::checkPrefix(Node *n, const Key &k,
                                          uint32_t &level) {
  if (n->hasPrefix()) {
    if (k.getKeyLen() <= level + n->getPrefixLength()) {
      return CheckPrefixResult::NoMatch;
    }
    for (uint32_t i = 0;
         i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
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

Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(
    Node *n, const Key &k, uint32_t &level, uint8_t &nonMatchingKey,
    Prefix &nonMatchingPrefix, KeyLoader keyLoader, bool &needRestart) {
  if (n->hasPrefix()) {
    uint32_t prevLevel = level;
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = Node::getAnyChildTid(n, needRestart);
        if (needRestart) return CheckPrefixPessimisticResult::Match;
        keyLoader.load(anyTID, kt);
      }
      uint8_t curKey =
          i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey != k[level]) {
        nonMatchingKey = curKey;
        if (n->getPrefixLength() > maxStoredPrefixLength) {
          if (i < maxStoredPrefixLength) {
            auto anyTID = Node::getAnyChildTid(n, needRestart);
            if (needRestart) return CheckPrefixPessimisticResult::Match;
            keyLoader.load(anyTID, kt);
          }
          memcpy(nonMatchingPrefix, &kt[0] + level + 1,
                 std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                          maxStoredPrefixLength));
        } else {
          memcpy(nonMatchingPrefix, n->getPrefix() + i + 1,
                 n->getPrefixLength() - i - 1);
        }
        return CheckPrefixPessimisticResult::NoMatch;
      }
      ++level;
    }
  }
  return CheckPrefixPessimisticResult::Match;
}

Tree::PCCompareResults Tree::checkPrefixCompare(const Node *n, const Key &k,
                                                uint8_t fillKey,
                                                uint32_t &level,
                                                KeyLoader keyLoader,
                                                bool &needRestart) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = Node::getAnyChildTid(n, needRestart);
        if (needRestart) return PCCompareResults::Equal;
        keyLoader.load(anyTID, kt);
      }
      uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

      uint8_t curKey =
          i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
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

Tree::PCEqualsResults Tree::checkPrefixEquals(const Node *n, uint32_t &level,
                                              const Key &start, const Key &end,
                                              KeyLoader keyLoader,
                                              bool &needRestart) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = Node::getAnyChildTid(n, needRestart);
        if (needRestart) return PCEqualsResults::BothMatch;
        keyLoader.load(anyTID, kt);
      }
      uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
      uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

      uint8_t curKey =
          i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
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

}  // namespace art
