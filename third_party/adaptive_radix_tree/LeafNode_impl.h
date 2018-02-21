#include "Node.h"

namespace art {

namespace {

enum class LeafNodeType : uint8_t {
  Inner = 0,
  Inlined_1 = 1,
  Inlined_2 = 2,
  External = 3,
};

const uint64_t kNumTagBits = 2;
const uint64_t kNumShiftBits = (sizeof(uint64_t) * 8ull) - kNumTagBits;

// Return the tagged type of the given node
LeafNodeType getType(const Node *n) {
  uint64_t type = reinterpret_cast<uint64_t>(n) >> kNumShiftBits;
  return static_cast<LeafNodeType>(type);
}

// Tag the given node
Node *setType(uint64_t n, LeafNodeType type) {
  uint64_t masked = n & ((1ul << kNumShiftBits) - 1);
  uint64_t tagged = masked | (static_cast<uint64_t>(type) << kNumShiftBits);
  return reinterpret_cast<Node *>(tagged);
}

void doDeleteLeaf(void *p) {
  auto *leaf = static_cast<LeafNode *>(p);
  free(leaf);
}

}  // anonymous namespace

//===----------------------------------------------------------------------===//
//
// LEAF TESTS
//
//===----------------------------------------------------------------------===//

bool LeafNode::isLeaf(const Node *n) {
  return getType(n) != LeafNodeType::Inner;
}

bool LeafNode::isInlined(const Node *n) {
  auto leaf_type = getType(n);
  return leaf_type == LeafNodeType::Inlined_1 ||
         leaf_type == LeafNodeType::Inlined_2;
}

bool LeafNode::isExternal(const Node *n) {
  return getType(n) == LeafNodeType::External;
}

//===----------------------------------------------------------------------===//
//
// POINTER TAGGING
//
//===----------------------------------------------------------------------===//

TID LeafNode::getInlined(const Node *n) {
  return reinterpret_cast<uint64_t>(n) & ((1ull << 63) - 1);
}

LeafNode *LeafNode::getExternal(const Node *n) {
  uint64_t untagged =
      reinterpret_cast<uint64_t>(n) & ((1ull << kNumShiftBits) - 1);
  return reinterpret_cast<LeafNode *>(untagged);
}

Node *LeafNode::setNonLeaf(const Node *n) {
  return setType(reinterpret_cast<uint64_t>(n), LeafNodeType::Inner);
}

Node *LeafNode::setInlined(TID tid) {
  bool hasHighBitSet = (tid & (1ull << kNumShiftBits)) != 0;
  if (hasHighBitSet) {
    return setType(tid, LeafNodeType::Inlined_1);
  } else {
    return setType(tid, LeafNodeType::Inlined_2);
  }
}

Node *LeafNode::setExternal(const LeafNode *n) {
  return setType(reinterpret_cast<uint64_t>(n), LeafNodeType::External);
}

//===----------------------------------------------------------------------===//
//
// LEAF CLEANUP
//
//===----------------------------------------------------------------------===//

void LeafNode::deleteLeaf(Node *n) {
  if (isExternal(n)) {
    auto *leaf = getExternal(n);
    free(leaf);
  }
}

//===----------------------------------------------------------------------===//
//
// LEAF ACCESS
//
//===----------------------------------------------------------------------===//

TID LeafNode::getLeaf(const Node *n) {
  assert(isLeaf(n));
  if (isInlined(n)) {
    return getInlined(n);
  } else {
    // No lock
    assert(isExternal(n));
    return getExternal(n)->getAnyNoLock();
  }
}

void LeafNode::readLeaf(const Node *n, std::vector<TID> &results,
                        bool &needRestart) {
  if (isInlined(n)) {
    results.push_back(getInlined(n));
    return;
  }

  // The leaf
  assert(isExternal(n));
  auto *leaf = getExternal(n);

  // Cache the size here in case we have to restart
  size_t sz = results.size();

  uint64_t v = leaf->readLockOrRestart(needRestart);
  if (needRestart) return;

  // Read
  leaf->getAll(results);

  leaf->readUnlockOrRestart(v, needRestart);
  if (needRestart) {
    results.resize(sz);
  }
}

bool LeafNode::insertGrow(Node *n, TID val,
                          std::function<bool(const void *)> predicate,
                          uint8_t parentKey, Node *parent, uint64_t pv,
                          bool &needRestart, ThreadInfo &threadInfo) {
  if (isInlined(n)) {
    TID tid = getLeaf(n);
    if (tid == val) {
      // Duplicate value
      return false;
    }
    parent->upgradeToWriteLockOrRestart(pv, needRestart);
    if (needRestart) return false;

    // Check predicate if one is provided
    if (predicate != nullptr &&
        predicate(reinterpret_cast<const void *>(tid))) {
      parent->writeUnlock();
      return false;
    }

    // We need to create a new external leaf
    auto *newLeaf = LeafNode::create(4);
    newLeaf->insertNoDupCheck(tid);
    newLeaf->insertNoDupCheck(val);
    Node::change(parent, parentKey, setExternal(newLeaf));
    parent->writeUnlock();

    return true;
  }

  assert(isExternal(n));

  auto *leaf = getExternal(n);

  uint64_t v = leaf->readLockOrRestart(needRestart);
  if (needRestart) return false;

  if (leaf->isFull()) {
    // We need to expand leaf, grab write locks on parent and leaf, create new
    // node, copy data, insert new data, exchange old leaf with new leaf in
    // parent, unlock everything.
    parent->upgradeToWriteLockOrRestart(pv, needRestart);
    if (needRestart) return false;

    leaf->upgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) {
      parent->writeUnlock();
      return false;
    }

    if (predicate != nullptr && leaf->check(predicate)) {
      leaf->writeUnlock();
      parent->writeUnlock();
      return false;
    }

    auto *newLeaf = LeafNode::create(leaf->capacity * 2);
    leaf->copyTo(newLeaf);
    bool inserted = newLeaf->insert(val);

    Node::change(parent, parentKey, setExternal(newLeaf));

    leaf->writeUnlockObsolete();
    threadInfo.getEpoch().markNodeForDeletion(leaf, doDeleteLeaf, threadInfo);
    parent->writeUnlock();

    return inserted;
  }

  // Upgrade to write lock, perform insertion and unlock
  leaf->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) return false;

  if (predicate != nullptr && leaf->check(predicate)) {
    leaf->writeUnlock();
    return false;
  }

  bool inserted = leaf->insert(val);

  leaf->writeUnlock();

  return inserted;
}

bool LeafNode::removeShrink(Node *n, TID val, uint8_t parentKey, Node *parent,
                            uint64_t pv, bool &needRestart,
                            ThreadInfo &threadInfo) {
  // Removal of an inlined leaf node must be handled outside this function
  // because we would need to know the node, its parent, its grand parent,
  // along with all versions and keys. Thus, we handle removal of inlined nodes
  // directly in Tree::remove() and enter here only for external leaves, since
  // these only locally affect the leaf and/or its parent.

  assert(isExternal(n));

  auto *leaf = getExternal(n);
  uint64_t v = leaf->readLockOrRestart(needRestart);
  if (needRestart) return false;

  // If the item we're removing isn't in the leaf, exit.
  int32_t pos = leaf->find(val);
  if (pos == -1) {
    leaf->readUnlockOrRestart(v, needRestart);
    return false;
  }

  // If the leaf is under-full, we'll inline the remaining TID into the parent
  if (leaf->count == 2) {
    parent->upgradeToWriteLockOrRestart(pv, needRestart);
    if (needRestart) return false;

    leaf->upgradeToWriteLockOrRestart(v, needRestart);
    if (needRestart) {
      parent->writeUnlock();
      return false;
    }

    TID second = leaf->vals[0] == val ? leaf->vals[1] : leaf->vals[0];
    Node::change(parent, parentKey, LeafNode::setInlined(second));

    leaf->writeUnlockObsolete();
    threadInfo.getEpoch().markNodeForDeletion(leaf, doDeleteLeaf, threadInfo);
    parent->writeUnlock();
    return true;
  }

  // It's a regular removal
  leaf->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) return false;

  leaf->removeAt(static_cast<uint32_t>(pos));

  leaf->writeUnlock();

  return true;
}

LeafNode *LeafNode::create(uint32_t capacity) {
  void *mem = malloc(sizeof(LeafNode) + (sizeof(TID) * capacity));
  assert(mem);
  return new (mem) LeafNode(capacity);
}

//===----------------------------------------------------------------------===//
//
// MEMBER FUNCTIONS - constructor, read, insert, remove, copy
//
//===----------------------------------------------------------------------===//

LeafNode::LeafNode(uint32_t _capacity)
    : lock(0), count(0), capacity(_capacity) {
  memset(vals, 0, sizeof(TID) * capacity);
}

TID LeafNode::getAnyNoLock() const {
  assert(count > 0);
  return vals[0];
}

void LeafNode::getAll(std::vector<TID> &results) const {
  for (uint32_t i = 0; i < count; i++) {
    results.push_back(vals[i]);
  }
}

bool LeafNode::insert(TID tid) {
  assert(!isFull());
  bool found = (find(tid) != -1);
  if (find(tid) == -1) {
    insertNoDupCheck(tid);
  }
  return !found;
}

void LeafNode::insertNoDupCheck(TID tid) {
  vals[count++] = tid;
}

int32_t LeafNode::find(TID tid) {
  for (uint32_t pos = 0; pos < count; pos++) {
    if (vals[pos] == tid) {
      return pos;
    }
  }
  return -1;
}

bool LeafNode::check(const std::function<bool(const void *)> &predicate) const {
  for (uint32_t pos = 0; pos < count; pos++) {
    if (predicate(reinterpret_cast<const void *>(vals[pos]))) {
      return true;
    }
  }
  return false;
}

bool LeafNode::remove(TID tid) {
  int32_t found_pos = find(tid);
  if (found_pos != -1) {
    removeAt(static_cast<uint32_t>(found_pos));
    return true;
  }
  return false;
}

void LeafNode::removeAt(uint32_t pos) {
  assert(count > 0);
  memmove(vals + pos, vals + pos + 1, (count - pos - 1) * sizeof(TID));
  count--;
}

void LeafNode::copyTo(LeafNode *other) const {
  for (uint32_t i = 0; i < count; i++) {
    other->insertNoDupCheck(vals[i]);
  }
}

}  // namespace art