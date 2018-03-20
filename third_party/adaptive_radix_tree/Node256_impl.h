#include <algorithm>

#include "Node.h"

namespace art {

bool Node256::isFull() const { return false; }

bool Node256::isUnderfull() const { return count == 37; }

void Node256::deleteChildren() {
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      Node::deleteChildren(children[i]);
      Node::deleteNode(children[i]);
    }
  }
}

void Node256::insert(uint8_t key, Node *val) {
  children[key] = val;
  count++;
}

bool Node256::change(uint8_t key, Node *n) {
  children[key] = n;
  return true;
}

Node *Node256::getChild(const uint8_t k) const { return children[k]; }

void Node256::remove(uint8_t k) {
  children[k] = nullptr;
  count--;
}

Node *Node256::getAnyChild() const {
  Node *anyChild = nullptr;
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != nullptr) {
      if (Node::isLeaf(children[i])) {
        return children[i];
      } else {
        anyChild = children[i];
      }
    }
  }
  return anyChild;
}

uint64_t Node256::getChildren(uint8_t start, uint8_t end,
                              std::tuple<uint8_t, Node *> *&children,
                              uint32_t &childrenCount, bool &needRestart) const {
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) return 0;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->children[i] != nullptr) {
      children[childrenCount] = std::make_tuple(i, this->children[i]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) return 0;
  return v;
}

}  // namespace art