//
// Created by florian on 05.08.15.
//

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

#include "Key.h"
#include "Epoch.h"

using TID = uint64_t;

namespace art {

enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

static constexpr uint32_t maxStoredPrefixLength = 11;

using Prefix = uint8_t[maxStoredPrefixLength];

class OptimisticRWLock {
 protected:
  // 2-bit type, 60-bit version, 1-bit lock, 1-bit obsolete flag
  // Initialized with version 1, not locked, not obsolete
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};

 public:
  explicit OptimisticRWLock(uint64_t type);

  // Is the provided version locked or obsolete?
  static bool isLocked(uint64_t version);
  static bool isObsolete(uint64_t version);

  uint64_t getType() const;

  // Read lock/unlock, checking if the process should be restarted
  uint64_t readLockOrRestart(bool &needRestart) const;
  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const;

  // Returns true if node hasn't been changed in between last version read
  void checkOrRestart(uint64_t startRead, bool &needRestart) const;

  // Lock upgrade from read lock to write lock
  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart);

  // Write lock/unlock
  void writeLockOrRestart(bool &needRestart);
  void writeUnlock();

  // This method unlocks the node and marks it obsolete simultaneously
  void writeUnlockObsolete();
};

//===----------------------------------------------------------------------===//
// Base Node class
//===----------------------------------------------------------------------===//
class Node {
 protected:
  OptimisticRWLock lock;
  uint32_t prefixCount = 0;
  uint8_t count = 0;
  Prefix prefix;

 protected:
  Node(NodeType type, const uint8_t *prefix, uint32_t prefixLength)
      : lock(static_cast<uint64_t>(type)) {
    setPrefix(prefix, prefixLength);
  }

  Node(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(const Node &) = delete;
  Node &operator=(Node &&) = delete;

 public:
  //===--------------------------------------------------------------------===//
  // MEMBER ACCESSORS
  //===--------------------------------------------------------------------===//

  NodeType getType() const;

  uint32_t getCount() const;

  bool hasPrefix() const;

  const uint8_t *getPrefix() const;

  void setPrefix(const uint8_t *prefix, uint32_t length);

  void addPrefixBefore(Node *node, uint8_t key);

  uint32_t getPrefixLength() const;

  //===--------------------------------------------------------------------===//
  // LOCKING
  //===--------------------------------------------------------------------===//
  uint64_t readLockOrRestart(bool &needRestart) const {
    return lock.readLockOrRestart(needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    lock.readUnlockOrRestart(startRead, needRestart);
  }

  // Returns true if node hasn't been changed in between last version read
  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    lock.checkOrRestart(startRead, needRestart);
  }

  // Lock upgrade from read lock to write lock
  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    lock.upgradeToWriteLockOrRestart(version, needRestart);
  }

  // Write lock/unlock
  void writeLockOrRestart(bool &needRestart) {
    lock.writeLockOrRestart(needRestart);
  }

  void writeUnlock() { lock.writeUnlock(); }

  // This method unlocks the node and marks it obsolete simultaneously
  void writeUnlockObsolete() { lock.writeUnlockObsolete(); }

  //===--------------------------------------------------------------------===//
  // NODE CLEANUP
  //===--------------------------------------------------------------------===//

  static void deleteChildren(Node *node);

  static void deleteNode(Node *node);

  //===--------------------------------------------------------------------===//
  // NODE ACCESS
  //===--------------------------------------------------------------------===//

  // Get the child of the provided node with the given key
  static Node *getChild(uint8_t k, const Node *node);

  // Get any child of the current node.
  static Node *getAnyChild(const Node *n);

  static TID getAnyChildTid(const Node *n, bool &needRestart);

  static std::tuple<Node *, uint8_t> getSecondChild(Node *node, uint8_t k);

  static uint64_t getChildren(const Node *node, uint8_t start, uint8_t end,
                              std::tuple<uint8_t, Node *> children[],
                              uint32_t &childrenCount, bool &needRestart);

  //===--------------------------------------------------------------------===//
  // LEAF MANIPULATION
  //===--------------------------------------------------------------------===//

  static bool isLeaf(const Node *n);

  static Node *setNonLeaf(const Node *n);
  static Node *setLeaf(TID tid);

  static TID getLeaf(const Node *n);

  //===--------------------------------------------------------------------===//
  // NODE MANIPULATION
  //===--------------------------------------------------------------------===//

  static void insertAndUnlock(Node *node, uint64_t v, Node *parentNode,
                              uint64_t parentVersion, uint8_t keyParent,
                              uint8_t key, Node *val, bool &needRestart,
                              ThreadInfo &threadInfo);

  static bool change(Node *node, uint8_t key, Node *val);

  static void removeAndUnlock(Node *node, uint64_t v, uint8_t key,
                              Node *parentNode, uint64_t parentVersion,
                              uint8_t keyParent, bool &needRestart,
                              ThreadInfo &threadInfo);

 private:
  template <typename curN, typename biggerN>
  static void insertGrow(curN *n, uint64_t v, Node *parentNode,
                         uint64_t parentVersion, uint8_t keyParent, uint8_t key,
                         Node *val, bool &needRestart, ThreadInfo &threadInfo);

  template <typename curN, typename smallerN>
  static void removeAndShrink(curN *n, uint64_t v, Node *parentNode,
                              uint64_t parentVersion, uint8_t keyParent,
                              uint8_t key, bool &needRestart,
                              ThreadInfo &threadInfo);
};

//===----------------------------------------------------------------------===//
// Node4 - Node with at most four children.
//===----------------------------------------------------------------------===//
class Node4 : public Node {
 public:
  uint8_t keys[4];
  Node *children[4];

 public:
  Node4(const uint8_t *prefix, uint32_t prefixLength)
      : Node(NodeType::N4, prefix, prefixLength) {
    memset(keys, 0, sizeof(keys));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, Node *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, Node *val);

  Node *getChild(uint8_t k) const;

  void remove(uint8_t k);

  Node *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  std::tuple<Node *, uint8_t> getSecondChild(uint8_t key) const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, Node *> *&children,
                       uint32_t &childrenCount, bool &needRestart) const;
};

//===----------------------------------------------------------------------===//
// Node16 - Node with at most 16 children.
//===----------------------------------------------------------------------===//
class Node16 : public Node {
 public:
  uint8_t keys[16];
  Node *children[16];

  static uint8_t flipSign(uint8_t keyByte) {
    // Flip the sign bit. Enables signed SSE comparison of unsigned values.
    return keyByte ^ 128;
  }

  static inline unsigned ctz(uint16_t x) {
// Count trailing zeros, only defined for x>0
#ifdef __GNUC__
    return __builtin_ctz(x);
#else
    // Adapted from Hacker's Delight
    unsigned n = 1;
    if ((x & 0xFF) == 0) {
      n += 8;
      x = x >> 8;
    }
    if ((x & 0x0F) == 0) {
      n += 4;
      x = x >> 4;
    }
    if ((x & 0x03) == 0) {
      n += 2;
      x = x >> 2;
    }
    return n - (x & 1);
#endif
  }

  Node *const *getChildPos(uint8_t k) const;

 public:
  Node16(const uint8_t *prefix, uint32_t prefixLength)
      : Node(NodeType::N16, prefix, prefixLength) {
    memset(keys, 0, sizeof(keys));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, Node *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, Node *val);

  Node *getChild(uint8_t k) const;

  void remove(uint8_t k);

  Node *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, Node *> *&children,
                       uint32_t &childrenCount, bool &needRestart) const;
};

//===----------------------------------------------------------------------===//
// Node48 - Node with at most 48 children.
//===----------------------------------------------------------------------===//
class Node48 : public Node {
  uint8_t childIndex[256];
  Node *children[48];

 public:
  static const uint8_t emptyMarker = 48;

  Node48(const uint8_t *prefix, uint32_t prefixLength)
      : Node(NodeType::N48, prefix, prefixLength) {
    memset(childIndex, emptyMarker, sizeof(childIndex));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, Node *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, Node *val);

  Node *getChild(uint8_t k) const;

  void remove(uint8_t k);

  Node *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, Node *> *&children,
                       uint32_t &childrenCount, bool &needRestart) const;
};

//===----------------------------------------------------------------------===//
// Node256 - Largest tree node supporting at most 256 children.
//===----------------------------------------------------------------------===//
class Node256 : public Node {
  Node *children[256];

 public:
  Node256(const uint8_t *prefix, uint32_t prefixLength)
      : Node(NodeType::N256, prefix, prefixLength) {
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, Node *val);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, Node *n);

  Node *getChild(uint8_t k) const;

  void remove(uint8_t k);

  Node *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, Node *> *&children,
                       uint32_t &childrenCount, bool &needRestart) const;
};

class LeafNode {
 private:
  OptimisticRWLock lock;
  uint32_t count;
  uint32_t capacity;
  TID vals[0];

 public:
  static bool isLeaf(const Node *n);
  static bool isInlined(const Node *n);
  static bool isExternal(const Node *n);

  static TID getInlined(const Node *n);
  static LeafNode *getExternal(const Node *n);

  static Node *setNonLeaf(const Node *n);
  static Node *setInlined(TID tid);
  static Node *setExternal(const LeafNode *n);

  static void deleteLeaf(Node *n);

  static TID getLeaf(const Node *n);
  static void readLeaf(const Node *n, std::vector<TID> &results,
                       bool &needRestart);
  static bool insertGrow(Node *n, TID val,
                         std::function<bool(const void *)> predicate,
                         uint8_t parentKey, Node *parent, uint64_t pv,
                         bool &needRestart, ThreadInfo &threadInfo);
  static bool removeShrink(Node *n, TID val, uint8_t parentKey, Node *parent,
                           uint64_t pv, bool &needRestart,
                           ThreadInfo &threadInfo);

  static LeafNode *create(uint32_t capacity);

 private:
  // Private constructor, use factory method
  explicit LeafNode(uint32_t capacity);

  //===--------------------------------------------------------------------===//
  // LOCKING
  //===--------------------------------------------------------------------===//

  uint64_t readLockOrRestart(bool &needRestart) const {
    return lock.readLockOrRestart(needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    lock.readUnlockOrRestart(startRead, needRestart);
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    lock.upgradeToWriteLockOrRestart(version, needRestart);
  }

  void writeUnlock() { lock.writeUnlock(); }

  void writeUnlockObsolete() { lock.writeUnlockObsolete(); }

 public:
  bool isFull() const { return count == capacity; }

  TID getAnyNoLock() const;

  void getAll(std::vector<TID> &results) const;

  int32_t find(TID tid);

  bool check(const std::function<bool(const void *)> &predicate) const;

  bool insert(TID tid);
  void insertNoDupCheck(TID tid);

  bool remove(TID tid);
  void removeAt(uint32_t pos);

  void copyTo(LeafNode *other) const;
};

template <class NODE>
inline void Node4::copyTo(NODE *n) const {
  for (uint32_t i = 0; i < count; ++i) {
    n->insert(keys[i], children[i]);
  }
}

template <class NODE>
inline void Node16::copyTo(NODE *n) const {
  for (unsigned i = 0; i < count; i++) {
    n->insert(flipSign(keys[i]), children[i]);
  }
}

template <class NODE>
inline void Node48::copyTo(NODE *n) const {
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      n->insert(i, children[childIndex[i]]);
    }
  }
}

template <class NODE>
inline void Node256::copyTo(NODE *n) const {
  for (int i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      n->insert(i, children[i]);
    }
  }
}

}  // namespace art
