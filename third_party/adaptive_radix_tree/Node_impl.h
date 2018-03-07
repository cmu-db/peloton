#include <cassert>
#include <algorithm>

#include "Node.h"
#include "Node4_impl.h"
#include "Node16_impl.h"
#include "Node48_impl.h"
#include "Node256_impl.h"
#include "LeafNode_impl.h"

namespace art {

//===----------------------------------------------------------------------===//
//
// Optimistic RW Lock
//
//===----------------------------------------------------------------------===//

bool OptimisticRWLock::isLocked(uint64_t version) {
  return ((version & 0b10) == 0b10);
}

bool OptimisticRWLock::isObsolete(uint64_t version) {
  return (version & 1) == 1;
}

OptimisticRWLock::OptimisticRWLock(uint64_t type) {
  typeVersionLockObsolete.fetch_add(type << 62);
}

uint64_t OptimisticRWLock::getType() const {
  return typeVersionLockObsolete.load(std::memory_order_relaxed) >> 62;
}

uint64_t OptimisticRWLock::readLockOrRestart(bool &needRestart) const {
  uint64_t version;
#if 0
  do {
    version = typeVersionLockObsolete.load();
  } while (isLocked(version));
  if (isObsolete(version)) {
    needRestart = true;
  }
  return version;
#endif
  version = typeVersionLockObsolete.load();
  if (isLocked(version) || isObsolete(version)) {
    needRestart = true;
  }
  return version;
}

void OptimisticRWLock::readUnlockOrRestart(uint64_t startRead,
                                           bool &needRestart) const {
  needRestart = (startRead != typeVersionLockObsolete.load());
}

void OptimisticRWLock::checkOrRestart(uint64_t startRead,
                                      bool &needRestart) const {
  readUnlockOrRestart(startRead, needRestart);
}

void OptimisticRWLock::upgradeToWriteLockOrRestart(uint64_t &version,
                                                   bool &needRestart) {
  if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                      version + 0b10)) {
    version = version + 0b10;
  } else {
    needRestart = true;
  }
}

void OptimisticRWLock::writeLockOrRestart(bool &needRestart) {
  uint64_t version = readLockOrRestart(needRestart);
  if (needRestart) return;

  upgradeToWriteLockOrRestart(version, needRestart);
  if (needRestart) return;
}

void OptimisticRWLock::writeUnlock() {
  typeVersionLockObsolete.fetch_add(0b10);
}

void OptimisticRWLock::writeUnlockObsolete() {
  typeVersionLockObsolete.fetch_add(0b11);
}

//===----------------------------------------------------------------------===//
//
// MEMBER ACCESSORS
//
//===----------------------------------------------------------------------===//

NodeType Node::getType() const {
  return static_cast<NodeType>(lock.getType());
}

uint32_t Node::getCount() const { return count; }

bool Node::hasPrefix() const { return prefixCount > 0; }

const uint8_t *Node::getPrefix() const { return prefix; }

void Node::setPrefix(const uint8_t *prefix, uint32_t length) {
  if (length > 0) {
    memcpy(this->prefix, prefix, std::min(length, maxStoredPrefixLength));
    prefixCount = length;
  } else {
    prefixCount = 0;
  }
}

uint32_t Node::getPrefixLength() const { return prefixCount; }

void Node::addPrefixBefore(Node *node, uint8_t key) {
  uint32_t prefixCopyCount =
      std::min(maxStoredPrefixLength, node->getPrefixLength() + 1);
  memmove(this->prefix + prefixCopyCount, this->prefix,
          std::min(this->getPrefixLength(),
                   maxStoredPrefixLength - prefixCopyCount));
  memcpy(this->prefix, node->prefix,
         std::min(prefixCopyCount, node->getPrefixLength()));
  if (node->getPrefixLength() < maxStoredPrefixLength) {
    this->prefix[prefixCopyCount - 1] = key;
  }
  this->prefixCount += node->getPrefixLength() + 1;
}

//===----------------------------------------------------------------------===//
//
// NODE CLEANUP
//
//===----------------------------------------------------------------------===//

void Node::deleteChildren(Node *node) {
  if (Node::isLeaf(node)) {
    return;
  }
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<Node4 *>(node);
      n->deleteChildren();
      return;
    }
    case NodeType::N16: {
      auto n = static_cast<Node16 *>(node);
      n->deleteChildren();
      return;
    }
    case NodeType::N48: {
      auto n = static_cast<Node48 *>(node);
      n->deleteChildren();
      return;
    }
    case NodeType::N256: {
      auto n = static_cast<Node256 *>(node);
      n->deleteChildren();
      return;
    }
  }
  assert(false);
  __builtin_unreachable();
}

void Node::deleteNode(Node *node) {
  if (Node::isLeaf(node)) {
    LeafNode::deleteLeaf(node);
    return;
  }
  switch (node->getType()) {
    case NodeType::N4: {
      auto *n = static_cast<Node4 *>(node);
      delete n;
      return;
    }
    case NodeType::N16: {
      auto *n = static_cast<Node16 *>(node);
      delete n;
      return;
    }
    case NodeType::N48: {
      auto *n = static_cast<Node48 *>(node);
      delete n;
      return;
    }
    case NodeType::N256: {
      auto *n = static_cast<Node256 *>(node);
      delete n;
      return;
    }
  }
  __builtin_unreachable();
}

//===----------------------------------------------------------------------===//
//
// NODE ACCESS
//
//===----------------------------------------------------------------------===//

Node *Node::getChild(const uint8_t k, const Node *node) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<const Node4 *>(node);
      return n->getChild(k);
    }
    case NodeType::N16: {
      auto n = static_cast<const Node16 *>(node);
      return n->getChild(k);
    }
    case NodeType::N48: {
      auto n = static_cast<const Node48 *>(node);
      return n->getChild(k);
    }
    case NodeType::N256: {
      auto n = static_cast<const Node256 *>(node);
      return n->getChild(k);
    }
  }
  assert(false);
  __builtin_unreachable();
}

Node *Node::getAnyChild(const Node *node) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<const Node4 *>(node);
      return n->getAnyChild();
    }
    case NodeType::N16: {
      auto n = static_cast<const Node16 *>(node);
      return n->getAnyChild();
    }
    case NodeType::N48: {
      auto n = static_cast<const Node48 *>(node);
      return n->getAnyChild();
    }
    case NodeType::N256: {
      auto n = static_cast<const Node256 *>(node);
      return n->getAnyChild();
    }
  }
  assert(false);
  __builtin_unreachable();
}

TID Node::getAnyChildTid(const Node *n, bool &needRestart) {
  const Node *nextNode = n;

  while (true) {
    const Node *node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) return 0;

    nextNode = getAnyChild(node);
    node->readUnlockOrRestart(v, needRestart);
    if (needRestart) return 0;

    assert(nextNode != nullptr);
    if (isLeaf(nextNode)) {
      return getLeaf(nextNode);
    }
  }
}

std::tuple<Node *, uint8_t> Node::getSecondChild(Node *node,
                                                 const uint8_t key) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<Node4 *>(node);
      return n->getSecondChild(key);
    }
    default: {
      assert(false);
      __builtin_unreachable();
    }
  }
}

uint64_t Node::getChildren(const Node *node, uint8_t start, uint8_t end,
                           std::tuple<uint8_t, Node *> children[],
                           uint32_t &childrenCount, bool &needRestart) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto *n = static_cast<const Node4 *>(node);
      return n->getChildren(start, end, children, childrenCount, needRestart);
    }
    case NodeType::N16: {
      auto *n = static_cast<const Node16 *>(node);
      return n->getChildren(start, end, children, childrenCount, needRestart);
    }
    case NodeType::N48: {
      auto *n = static_cast<const Node48 *>(node);
      return n->getChildren(start, end, children, childrenCount, needRestart);
    }
    case NodeType::N256: {
      auto *n = static_cast<const Node256 *>(node);
      return n->getChildren(start, end, children, childrenCount, needRestart);
    }
  }
  assert(false);
  __builtin_unreachable();
}

//===----------------------------------------------------------------------===//
//
// LEAF MANIPULATION
//
//===----------------------------------------------------------------------===//

bool Node::isLeaf(const Node *n) { return LeafNode::isLeaf(n); }

Node *Node::setNonLeaf(const Node *n) { return LeafNode::setNonLeaf(n); }

Node *Node::setLeaf(TID tid) { return LeafNode::setInlined(tid); }

TID Node::getLeaf(const Node *n) { return LeafNode::getLeaf(n); }

//===----------------------------------------------------------------------===//
//
// NODE MANIPULATION
//
//===----------------------------------------------------------------------===//

template <typename CurNodeType, typename BiggerNodeType>
void Node::insertGrow(CurNodeType *n, uint64_t v, Node *parentNode,
                      uint64_t parentVersion, uint8_t keyParent, uint8_t key,
                      Node *val, bool &needRestart, ThreadInfo &threadInfo) {
  if (!n->isFull()) {
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    n->upgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) return;
    n->insert(key, val);
    n->writeUnlock();
    return;
  }

  parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) return;

  n->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) {
    parentNode->writeUnlock();
    return;
  }

  auto nBig = new BiggerNodeType(n->getPrefix(), n->getPrefixLength());
  n->copyTo(nBig);
  nBig->insert(key, val);

  Node::change(parentNode, keyParent, Node::setNonLeaf(nBig));

  n->writeUnlockObsolete();
  threadInfo.getEpoch().markNodeForDeletion(n, threadInfo);
  parentNode->writeUnlock();
}

template <typename CurNodeType, typename SmallerNodeType>
void Node::removeAndShrink(CurNodeType *n, uint64_t v, Node *parentNode,
                           uint64_t parentVersion, uint8_t keyParent,
                           uint8_t key, bool &needRestart,
                           ThreadInfo &threadInfo) {
  if (!n->isUnderfull() || parentNode == nullptr) {
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    n->upgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) return;

    n->remove(key);
    n->writeUnlock();
    return;
  }
  parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) return;

  n->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) {
    parentNode->writeUnlock();
    return;
  }

  auto nSmall = new SmallerNodeType(n->getPrefix(), n->getPrefixLength());

  n->copyTo(nSmall);
  nSmall->remove(key);
  Node::change(parentNode, keyParent, Node::setNonLeaf(nSmall));

  n->writeUnlockObsolete();
  threadInfo.getEpoch().markNodeForDeletion(n, threadInfo);
  parentNode->writeUnlock();
}

// We specialize the element removal for Node4. If we're removing the
// second-last element from a Node4, we need to insert the last element into the
// parent for prefix compression.
template <>
void Node::removeAndShrink<Node4, Node4>(Node4 *n, uint64_t v, Node *parentNode,
                                         uint64_t parentVersion,
                                         uint8_t keyParent, uint8_t key,
                                         bool &needRestart,
                                         ThreadInfo &threadInfo) {
  if (n->getCount() > 2 || parentNode == nullptr) {
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    n->upgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) return;

    n->remove(key);
    n->writeUnlock();
    return;
  }

  parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
  if (needRestart) return;

  n->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) {
    parentNode->writeUnlock();
    return;
  }

  // 1. check remaining entries
  Node *secondNodeN;
  uint8_t secondNodeK;
  std::tie(secondNodeN, secondNodeK) = Node::getSecondChild(n, key);
  if (Node::isLeaf(secondNodeN)) {
    // N::remove(node, k[level]); not necessary
    Node::change(parentNode, keyParent, secondNodeN);
    parentNode->writeUnlock();

    n->writeUnlockObsolete();
    threadInfo.getEpoch().markNodeForDeletion(n, threadInfo);
  } else {
    secondNodeN->writeLockOrRestart(needRestart);
    if (needRestart) {
      n->writeUnlock();
      parentNode->writeUnlock();
      return;
    }

    // N::remove(node, k[level]); not necessary
    Node::change(parentNode, keyParent, secondNodeN);
    parentNode->writeUnlock();

    secondNodeN->addPrefixBefore(n, secondNodeK);
    secondNodeN->writeUnlock();

    n->writeUnlockObsolete();
    threadInfo.getEpoch().markNodeForDeletion(n, threadInfo);
  }
}

void Node::insertAndUnlock(Node *node, uint64_t v, Node *parentNode,
                           uint64_t parentVersion, uint8_t keyParent,
                           uint8_t key, Node *val, bool &needRestart,
                           ThreadInfo &threadInfo) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<Node4 *>(node);
      insertGrow<Node4, Node16>(n, v, parentNode, parentVersion, keyParent, key,
                                val, needRestart, threadInfo);
      break;
    }
    case NodeType::N16: {
      auto n = static_cast<Node16 *>(node);
      insertGrow<Node16, Node48>(n, v, parentNode, parentVersion, keyParent,
                                 key, val, needRestart, threadInfo);
      break;
    }
    case NodeType::N48: {
      auto n = static_cast<Node48 *>(node);
      insertGrow<Node48, Node256>(n, v, parentNode, parentVersion, keyParent,
                                  key, val, needRestart, threadInfo);
      break;
    }
    case NodeType::N256: {
      auto n = static_cast<Node256 *>(node);
      insertGrow<Node256, Node256>(n, v, parentNode, parentVersion, keyParent,
                                   key, val, needRestart, threadInfo);
      break;
    }
  }
}

bool Node::change(Node *node, uint8_t key, Node *val) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<Node4 *>(node);
      return n->change(key, val);
    }
    case NodeType::N16: {
      auto n = static_cast<Node16 *>(node);
      return n->change(key, val);
    }
    case NodeType::N48: {
      auto n = static_cast<Node48 *>(node);
      return n->change(key, val);
    }
    case NodeType::N256: {
      auto n = static_cast<Node256 *>(node);
      return n->change(key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

void Node::removeAndUnlock(Node *node, uint64_t v, uint8_t key,
                           Node *parentNode, uint64_t parentVersion,
                           uint8_t keyParent, bool &needRestart,
                           ThreadInfo &threadInfo) {
  switch (node->getType()) {
    case NodeType::N4: {
      auto n = static_cast<Node4 *>(node);
      removeAndShrink<Node4, Node4>(n, v, parentNode, parentVersion, keyParent,
                                    key, needRestart, threadInfo);
      break;
    }
    case NodeType::N16: {
      auto n = static_cast<Node16 *>(node);
      removeAndShrink<Node16, Node4>(n, v, parentNode, parentVersion, keyParent,
                                     key, needRestart, threadInfo);
      break;
    }
    case NodeType::N48: {
      auto n = static_cast<Node48 *>(node);
      removeAndShrink<Node48, Node16>(n, v, parentNode, parentVersion,
                                      keyParent, key, needRestart, threadInfo);
      break;
    }
    case NodeType::N256: {
      auto n = static_cast<Node256 *>(node);
      removeAndShrink<Node256, Node48>(n, v, parentNode, parentVersion,
                                       keyParent, key, needRestart, threadInfo);
      break;
    }
  }
}

}  // namespace art