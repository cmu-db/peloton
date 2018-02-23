#include <cassert>
#include <algorithm>
#include <emmintrin.h>  // x86 SSE intrinsics

#include "Node.h"

namespace art {

bool Node16::isFull() const { return count == 16; }

bool Node16::isUnderfull() const { return count == 3; }

void Node16::insert(uint8_t key, Node *n) {
  uint8_t keyByteFlipped = flipSign(key);
  __m128i cmp =
      _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped),
                     _mm_loadu_si128(reinterpret_cast<__m128i *>(keys)));
  uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - count));
  unsigned pos = bitfield ? ctz(bitfield) : count;
  memmove(keys + pos + 1, keys + pos, count - pos);
  memmove(children + pos + 1, children + pos,
          (count - pos) * sizeof(uintptr_t));
  keys[pos] = keyByteFlipped;
  children[pos] = n;
  count++;
}

bool Node16::change(uint8_t key, Node *val) {
  Node **childPos = const_cast<Node **>(getChildPos(key));
  assert(childPos != nullptr);
  *childPos = val;
  return true;
}

Node *const *Node16::getChildPos(const uint8_t k) const {
  __m128i cmp =
      _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
  unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count) - 1);
  if (bitfield) {
    return &children[ctz(bitfield)];
  } else {
    return nullptr;
  }
}

Node *Node16::getChild(const uint8_t k) const {
  Node *const *childPos = getChildPos(k);
  if (childPos == nullptr) {
    return nullptr;
  } else {
    return *childPos;
  }
}

void Node16::remove(uint8_t k) {
  Node *const *leafPlace = getChildPos(k);
  assert(leafPlace != nullptr);
  std::size_t pos = leafPlace - children;
  memmove(keys + pos, keys + pos + 1, count - pos - 1);
  memmove(children + pos, children + pos + 1,
          (count - pos - 1) * sizeof(Node *));
  count--;
  assert(getChild(k) == nullptr);
}

Node *Node16::getAnyChild() const {
  for (int i = 0; i < count; ++i) {
    if (Node::isLeaf(children[i])) {
      return children[i];
    }
  }
  return children[0];
}

void Node16::deleteChildren() {
  for (std::size_t i = 0; i < count; ++i) {
    Node::deleteChildren(children[i]);
    Node::deleteNode(children[i]);
  }
}

uint64_t Node16::getChildren(uint8_t start, uint8_t end,
                             std::tuple<uint8_t, Node *> *&children,
                             uint32_t &childrenCount, bool &needRestart) const {
  uint64_t v;
  v = readLockOrRestart(needRestart);
  if (needRestart) return 0;
  childrenCount = 0;
  auto startPos = getChildPos(start);
  auto endPos = getChildPos(end);
  if (startPos == nullptr) {
    startPos = this->children;
  }
  if (endPos == nullptr) {
    endPos = this->children + (count - 1);
  }
  for (auto p = startPos; p <= endPos; ++p) {
    children[childrenCount] =
        std::make_tuple(flipSign(keys[p - this->children]), *p);
    childrenCount++;
  }
  readUnlockOrRestart(v, needRestart);
  if (needRestart) return 0;
  return v;
}

}  // namespace art