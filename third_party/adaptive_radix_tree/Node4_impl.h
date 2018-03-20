#include <cassert>
#include <algorithm>

#include "Node.h"

namespace art {

void Node4::deleteChildren() {
  for (uint32_t i = 0; i < count; ++i) {
    Node::deleteChildren(children[i]);
    Node::deleteNode(children[i]);
  }
}

bool Node4::isFull() const { return count == 4; }

bool Node4::isUnderfull() const { return false; }

void Node4::insert(uint8_t key, Node *n) {
  unsigned pos;
  for (pos = 0; (pos < count) && (keys[pos] < key); pos++)
    ;
  memmove(keys + pos + 1, keys + pos, count - pos);
  memmove(children + pos + 1, children + pos, (count - pos) * sizeof(Node *));
  keys[pos] = key;
  children[pos] = n;
  count++;
}

bool Node4::change(uint8_t key, Node *val) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == key) {
      children[i] = val;
      return true;
    }
  }
  assert(false);
  __builtin_unreachable();
}

Node *Node4::getChild(const uint8_t k) const {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == k) {
      return children[i];
    }
  }
  return nullptr;
}

void Node4::remove(uint8_t k) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == k) {
      memmove(keys + i, keys + i + 1, count - i - 1);
      memmove(children + i, children + i + 1, (count - i - 1) * sizeof(Node *));
      count--;
      return;
    }
  }
}

Node *Node4::getAnyChild() const {
  Node *anyChild = nullptr;
  for (uint32_t i = 0; i < count; ++i) {
    if (Node::isLeaf(children[i])) {
      return children[i];
    } else {
      anyChild = children[i];
    }
  }
  return anyChild;
}

std::tuple<Node *, uint8_t> Node4::getSecondChild(const uint8_t key) const {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] != key) {
      return std::make_tuple(children[i], keys[i]);
    }
  }
  return std::make_tuple(nullptr, 0);
}

uint64_t Node4::getChildren(uint8_t start, uint8_t end,
                            std::tuple<uint8_t, Node *> *&children,
                            uint32_t &childrenCount, bool &needRestart) const {
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) return 0;
  childrenCount = 0;
  for (uint32_t i = 0; i < count; ++i) {
    if (this->keys[i] >= start && this->keys[i] <= end) {
      children[childrenCount] =
          std::make_tuple(this->keys[i], this->children[i]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) return 0;
  return v;
}

}  // namespace art