#include <cassert>
#include <algorithm>

#include "Node.h"

namespace art {

bool Node48::isFull() const { return count == 48; }

bool Node48::isUnderfull() const { return count == 12; }

void Node48::insert(uint8_t key, Node *n) {
  unsigned pos = count;
  if (children[pos]) {
    for (pos = 0; children[pos] != nullptr; pos++)
      ;
  }
  children[pos] = n;
  childIndex[key] = (uint8_t)pos;
  count++;
}

bool Node48::change(uint8_t key, Node *val) {
  children[childIndex[key]] = val;
  return true;
}

Node *Node48::getChild(const uint8_t k) const {
  if (childIndex[k] == emptyMarker) {
    return nullptr;
  } else {
    return children[childIndex[k]];
  }
}

void Node48::remove(uint8_t k) {
  assert(childIndex[k] != emptyMarker);
  children[childIndex[k]] = nullptr;
  childIndex[k] = emptyMarker;
  count--;
  assert(getChild(k) == nullptr);
}

Node *Node48::getAnyChild() const {
  Node *anyChild = nullptr;
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      if (Node::isLeaf(children[childIndex[i]])) {
        return children[childIndex[i]];
      } else {
        anyChild = children[childIndex[i]];
      };
    }
  }
  return anyChild;
}

void Node48::deleteChildren() {
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      Node::deleteChildren(children[childIndex[i]]);
      Node::deleteNode(children[childIndex[i]]);
    }
  }
}

uint64_t Node48::getChildren(uint8_t start, uint8_t end,
                             std::tuple<uint8_t, Node *> *&children,
                             uint32_t &childrenCount, bool &needRestart) const {
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) return 0;
  childrenCount = 0;
  for (unsigned i = start; i <= end; i++) {
    if (this->childIndex[i] != emptyMarker) {
      children[childrenCount] =
          std::make_tuple(i, this->children[this->childIndex[i]]);
      childrenCount++;
    }
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) return 0;
  return v;
}

}  // namespace art